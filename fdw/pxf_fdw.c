/*
 * pxf_fdw.c
 *		  Foreign-data wrapper for PXF (Platform Extension Framework)
 *
 * IDENTIFICATION
 *		  fdw/pxf_fdw.c
 */

#include "postgres.h"

#include "pxf_fdw.h"
#include "pxf_bridge.h"
#include "pxf_filter.h"

#include "access/reloptions.h"
#if PG_VERSION_NUM >= 90600
#include "access/table.h"
#endif
#include "cdb/cdbsreh.h"
#include "cdb/cdbvars.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/pg_list.h"
#if PG_VERSION_NUM >= 90600
#include "optimizer/optimizer.h"
#endif
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#if PG_VERSION_NUM < 90600
#include "optimizer/var.h"
#endif
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

#define DEFAULT_PXF_FDW_STARTUP_COST   50000

/*
 * Error token embedded in the data sent by PXF as part of an error row
 */
#define PXF_ERROR_TOKEN "PXFERRMSG> "
#define PXF_ERROR_TOKEN_SIZE strlen(PXF_ERROR_TOKEN)

extern Datum pxf_fdw_handler(PG_FUNCTION_ARGS);

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(pxf_fdw_handler);

/*
 * FDW functions declarations
 */
static void pxfGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);

static void pxfGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);

#if (PG_VERSION_NUM <= 90500)

static ForeignScan *pxfGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist,
									  List *scan_clauses);

#else
static ForeignScan *pxfGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan);
#endif

static void pxfExplainForeignScan(ForeignScanState *node, ExplainState *es);

static void pxfBeginForeignScan(ForeignScanState *node, int eflags);

static TupleTableSlot *pxfIterateForeignScan(ForeignScanState *node);

static void pxfReScanForeignScan(ForeignScanState *node);

static void pxfEndForeignScan(ForeignScanState *node);

/* Foreign updates */
static void pxfBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo, List *fdw_private, int subplan_index, int eflags);
static TupleTableSlot *pxfExecForeignInsert(EState *estate, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, TupleTableSlot *planSlot);

static void pxfEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo);

static int	pxfIsForeignRelUpdatable(Relation rel);

/*
 * Helper functions
 */
static void InitCopyState(PxfFdwScanState *pxfsstate);
static void InitCopyStateForModify(PxfFdwModifyState *pxfmstate);
static CopyState BeginCopyTo(Relation forrel, List *options);
static void PxfBeginScanErrorCallback(void *arg);
static void PxfCopyFromErrorCallback(void *arg);

/*
 * Foreign-data wrapper handler functions:
 * returns a struct with pointers to the
 * pxf_fdw callback routines.
 */
Datum
pxf_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdw_routine = makeNode(FdwRoutine);

	/*
	 * foreign table scan support
	 */

	/* master - only */
	fdw_routine->GetForeignRelSize = pxfGetForeignRelSize;
	fdw_routine->GetForeignPaths = pxfGetForeignPaths;
	fdw_routine->GetForeignPlan = pxfGetForeignPlan;
	fdw_routine->ExplainForeignScan = pxfExplainForeignScan;

	/* segment - only when mpp_execute = segments */
	fdw_routine->BeginForeignScan = pxfBeginForeignScan;
	fdw_routine->IterateForeignScan = pxfIterateForeignScan;
	fdw_routine->ReScanForeignScan = pxfReScanForeignScan;
	fdw_routine->EndForeignScan = pxfEndForeignScan;

	/*
	 * foreign table insert support
	 */

	/*
	 * AddForeignUpdateTargets set to NULL, no extra target expressions are
	 * added
	 */
	fdw_routine->AddForeignUpdateTargets = NULL;

	/*
	 * PlanForeignModify set to NULL, no additional plan-time actions are
	 * taken
	 */
	fdw_routine->PlanForeignModify = NULL;
	fdw_routine->BeginForeignModify = pxfBeginForeignModify;
	fdw_routine->ExecForeignInsert = pxfExecForeignInsert;

	/*
	 * ExecForeignUpdate and ExecForeignDelete set to NULL since updates and
	 * deletes are not supported
	 */
	fdw_routine->ExecForeignUpdate = NULL;
	fdw_routine->ExecForeignDelete = NULL;
	fdw_routine->EndForeignModify = pxfEndForeignModify;
	fdw_routine->IsForeignRelUpdatable = pxfIsForeignRelUpdatable;

	PG_RETURN_POINTER(fdw_routine);
}

typedef struct PxfFdwRelationInfo
{
	/* baserestrictinfo clauses, broken down into safe and unsafe subsets. */
	List	   *remote_conds;
	List	   *local_conds;

	/* List of attributes (columns) that we need to get */
	List	   *retrieved_attrs;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset  *attrs_used;
}			PxfFdwRelationInfo;

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * We store various information in ForeignScan.fdw_private to pass it from
 * planner to executor.  Currently we store:
 *
 * 1) WHERE clause text to be sent to the remote server
 * 2) Integer list of attribute numbers retrieved by the SELECT
 *
 * These items are indexed with the enum FdwScanPrivateIndex, so an item
 * can be fetched with list_nth().  For example, to get the WHERE clauses:
 *		sql = strVal(list_nth(fdw_private, FdwScanPrivateWhereClauses));
 */
enum FdwScanPrivateIndex
{
	/* WHERE clauses to be sent to PXF (as a String node) */
	FdwScanPrivateWhereClauses,
	/* Integer list of attribute numbers retrieved by the SELECT */
	FdwScanPrivateRetrievedAttrs
};

/*
 * GetForeignRelSize
 *		set relation size estimates for a foreign table
 */
static void
pxfGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	elog(DEBUG5, "pxf_fdw: pxfGetForeignRelSize starts on segment: %d", PXF_SEGMENT_ID);
	Relation	rel;
	ListCell   *lc;

	PxfFdwRelationInfo *fpinfo = (PxfFdwRelationInfo *) palloc(sizeof(PxfFdwRelationInfo));

	baserel->fdw_private = (void *) fpinfo;

	fpinfo->attrs_used = NULL;

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);

#if PG_VERSION_NUM >= 90600
	rel = table_open(rte->relid, NoLock);
#else
	rel = heap_open(rte->relid, NoLock);
#endif

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	classifyConditions(root, baserel, baserel->baserestrictinfo,
					   &fpinfo->remote_conds, &fpinfo->local_conds);

	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server
	 */
#if (PG_VERSION_NUM <= 90500)
	pull_varattnos((Node *) baserel->reltargetlist, baserel->relid, &fpinfo->attrs_used);
#else
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid, &fpinfo->attrs_used);
#endif
	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid, &fpinfo->attrs_used);
	}

	deparseTargetList(rel, fpinfo->attrs_used, &fpinfo->retrieved_attrs);

	heap_close(rel, NoLock);

	/* Use an artificial number of estimated rows */
	baserel->rows = 1000;

	elog(DEBUG5, "pxf_fdw: pxfGetForeignRelSize ends on segment: %d", PXF_SEGMENT_ID);
}

/*
 * GetForeignPaths
 *		create access path for a scan on the foreign table
 */
static void
pxfGetForeignPaths(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid)
{
	ForeignPath *path = NULL;
	int			total_cost = DEFAULT_PXF_FDW_STARTUP_COST;
	PxfFdwRelationInfo *fpinfo = (PxfFdwRelationInfo *) baserel->fdw_private;


	elog(DEBUG5, "pxf_fdw: pxfGetForeignPaths starts on segment: %d", PXF_SEGMENT_ID);


	path = create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
								   NULL,	/* default pathtarget */
#endif
								   baserel->rows,
								   DEFAULT_PXF_FDW_STARTUP_COST,
								   total_cost,
								   NIL, /* no pathkeys */
								   NULL,	/* no outer rel either */
#if PG_VERSION_NUM >= 90500
								   NULL,	/* no extra plan */
#endif
								   fpinfo->retrieved_attrs);



	/*
	 * Create a ForeignPath node and add it as only possible path.
	 */
	add_path(baserel, (Path *) path);

	elog(DEBUG5, "pxf_fdw: pxfGetForeignPaths ends on segment: %d", PXF_SEGMENT_ID);
}

/*
 * GetForeignPlan
 *		create a ForeignScan plan node
 */
#if PG_VERSION_NUM >= 90500
static ForeignScan *
pxfGetForeignPlan(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid,
				  ForeignPath *best_path,
				  List *tlist,
				  List *scan_clauses,
				  Plan *outer_plan)
#else
static ForeignScan *
pxfGetForeignPlan(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid,
				  ForeignPath *best_path,
				  List *tlist,	/* target list */
				  List *scan_clauses)
#endif
{
	char			   *where_clauses_str = NULL;
	List			   *fdw_private;
	Index				scan_relid = baserel->relid;
	PxfFdwRelationInfo *fpinfo = (PxfFdwRelationInfo *) baserel->fdw_private;
	PxfOptions		   *options = PxfGetOptions(foreigntableid);

	elog(DEBUG5, "pxf_fdw: pxfGetForeignPlan starts on segment: %d", PXF_SEGMENT_ID);

	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scan_clauses into the plan node's qual list for the
	 * executor to check.  So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	if (!options->disable_ppd)
	{
		/* here we serialize the WHERE clauses */
		where_clauses_str = SerializePxfFilterQuals(fpinfo->remote_conds);
	}

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwScanPrivateIndex, above.
	 */
	fdw_private = list_make2(makeString(where_clauses_str), fpinfo->retrieved_attrs);

	elog(DEBUG5, "pxf_fdw: pxfGetForeignPlan ends on segment: %d", PXF_SEGMENT_ID);

	return make_foreignscan(
							tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							fdw_private
#if PG_VERSION_NUM >= 90500
							,NIL
							,NIL
							,outer_plan
#endif
		);

}

/*
 * pxfExplainForeignScan
 *		Produce extra output for EXPLAIN of a ForeignScan on a foreign table
 */
static void
pxfExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	elog(DEBUG5, "pxf_fdw: pxfExplainForeignScan starts on segment: %d", PXF_SEGMENT_ID);

	/* TODO: make this a meaningful callback */

	elog(DEBUG5, "pxf_fdw: pxfExplainForeignScan ends on segment: %d", PXF_SEGMENT_ID);
}

/*
 * BeginForeignScan
 *   called during executor startup. perform any initialization
 *   needed, but not start the actual scan.
 */
static void
pxfBeginForeignScan(ForeignScanState *node, int eflags)
{
	elog(DEBUG5, "pxf_fdw: pxfBeginForeignScan starts on segment: %d", PXF_SEGMENT_ID);

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

#if PG_VERSION_NUM >= 90600
	ExprState  *quals             = node->ss.ps.qual;
#else
	List	   *quals             = node->ss.ps.qual;
#endif
	Oid			foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
	PxfFdwScanState *pxfsstate    = NULL;
	Relation	relation          = node->ss.ss_currentRelation;
	ForeignScan *foreignScan      = (ForeignScan *) node->ss.ps.plan;
	PxfOptions *options           = PxfGetOptions(foreigntableid);

	/* retrieve fdw-private information from pxfGetForeignPlan() */
	char *filter_str              = strVal(list_nth(foreignScan->fdw_private, FdwScanPrivateWhereClauses));
	List *retrieved_attrs = (List *) list_nth(foreignScan->fdw_private, FdwScanPrivateRetrievedAttrs);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		return;
	}

	/*
	 * Save state in node->fdw_state.  We must save enough information to call
	 * BeginCopyFrom() again.
	 */
	pxfsstate = (PxfFdwScanState *) palloc(sizeof(PxfFdwScanState));
	initStringInfo(&pxfsstate->uri);

	pxfsstate->filter_str = filter_str;
	pxfsstate->options = options;
	pxfsstate->quals = quals;
	pxfsstate->relation = relation;
	pxfsstate->retrieved_attrs = retrieved_attrs;
	pxfsstate->projectionInfo = node->ss.ps.ps_ProjInfo;

    /* Set up callback to identify error foreign relation. */
    ErrorContextCallback errcallback;
    errcallback.callback = PxfBeginScanErrorCallback;
    errcallback.arg = (void *) relation;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

	InitCopyState(pxfsstate);
	node->fdw_state = (void *) pxfsstate;

    /* Restore the previous error callback */
    error_context_stack = errcallback.previous;

    elog(DEBUG5, "pxf_fdw: pxfBeginForeignScan ends on segment: %d", PXF_SEGMENT_ID);
}

/*
 * IterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 *   Fetch one row from the foreign source, returning it in a tuple table slot
 *    (the node's ScanTupleSlot should be used for this purpose).
 *  Return NULL if no more rows are available.
 */
static TupleTableSlot *
pxfIterateForeignScan(ForeignScanState *node)
{
	elog(DEBUG5, "pxf_fdw: pxfIterateForeignScan Executing on segment: %d", PXF_SEGMENT_ID);

	PxfFdwScanState *pxfsstate = (PxfFdwScanState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	ErrorContextCallback errcallback;
	bool		found;

	/* Set up callback to identify error line number. */
	errcallback.callback = PxfCopyFromErrorCallback;
	errcallback.arg = (void *) pxfsstate;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/*
	 * The protocol for loading a virtual tuple into a slot is first
	 * ExecClearTuple, then fill the values/isnull arrays, then
	 * ExecStoreVirtualTuple.  If we don't find another row in the file, we
	 * just skip the last step, leaving the slot empty as required.
	 *
	 * We can pass ExprContext = NULL because we read all columns from the
	 * file, so no need to evaluate default expressions.
	 *
	 * We can also pass tupleOid = NULL because we don't allow oids for
	 * foreign tables.
	 */
	ExecClearTuple(slot);

	found = NextCopyFrom(pxfsstate->cstate,
						 NULL,
#if PG_VERSION_NUM >= 90600
						 slot->tts_values,
						 slot->tts_isnull
#else
						 slot_get_values(slot),
						 slot_get_isnull(slot),
						 NULL
#endif
						 );

	if (found)
	{
		if (pxfsstate->cstate->cdbsreh)
		{
			/*
			 * If NextCopyFrom failed, the processed row count will have
			 * already been updated, but we need to update it in a successful
			 * case.
			 *
			 * GPDB_91_MERGE_FIXME: this is almost certainly not the right
			 * place for this, but row counts are currently scattered all over
			 * the place. Consolidate.
			 */
			pxfsstate->cstate->cdbsreh->processed++;
		}

		ExecStoreVirtualTuple(slot);
	}

	/* Remove error callback. */
	error_context_stack = errcallback.previous;

	return slot;
}

/*
 * ReScanForeignScan
 *		Restart the scan from the beginning
 */
static void
pxfReScanForeignScan(ForeignScanState *node)
{
	elog(DEBUG5, "pxf_fdw: pxfReScanForeignScan starts on segment: %d", PXF_SEGMENT_ID);

	PxfFdwScanState *pxfsstate = (PxfFdwScanState *) node->fdw_state;

	EndCopyFrom(pxfsstate->cstate);
	InitCopyState(pxfsstate);

	elog(DEBUG5, "pxf_fdw: pxfReScanForeignScan ends on segment: %d", PXF_SEGMENT_ID);
}

/*
 * EndForeignScan
 *		End the scan and release resources.
 */
static void
pxfEndForeignScan(ForeignScanState *node)
{
	elog(DEBUG5, "pxf_fdw: pxfEndForeignScan starts on segment: %d", PXF_SEGMENT_ID);

	ForeignScan *foreignScan = (ForeignScan *) node->ss.ps.plan;
	PxfFdwScanState *pxfsstate = (PxfFdwScanState *) node->fdw_state;

	/* Release resources */
	if (foreignScan->fdw_private)
	{
		elog(DEBUG5, "Freeing fdw_private");
		pfree(foreignScan->fdw_private);
	}

	/* if pxfsstate is NULL, we are in EXPLAIN; nothing to do */
	if (pxfsstate)
		EndCopyFrom(pxfsstate->cstate);

	elog(DEBUG5, "pxf_fdw: pxfEndForeignScan ends on segment: %d", PXF_SEGMENT_ID);
}

/*
 * pxfBeginForeignModify
 *		Begin an insert/update/delete operation on a foreign table
 */
static void
pxfBeginForeignModify(ModifyTableState *mtstate,
					  ResultRelInfo *resultRelInfo,
					  List *fdw_private,
					  int subplan_index,
					  int eflags)
{
	elog(DEBUG5, "pxf_fdw: pxfBeginForeignModify starts on segment: %d", PXF_SEGMENT_ID);

	ForeignTable *rel;
	Oid			foreigntableid;
	PxfOptions *options = NULL;
	PxfFdwModifyState *pxfmstate = NULL;
	Relation	relation = resultRelInfo->ri_RelationDesc;
	TupleDesc	tupDesc;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	foreigntableid = RelationGetRelid(relation);
	rel = GetForeignTable(foreigntableid);

	if (Gp_role == GP_ROLE_DISPATCH && rel->exec_location == FTEXECLOCATION_ALL_SEGMENTS)
		/* master does not process any data when exec_location is all segments */
		return;

	tupDesc = RelationGetDescr(relation);
	options = PxfGetOptions(foreigntableid);
	pxfmstate = palloc(sizeof(PxfFdwModifyState));

	initStringInfo(&pxfmstate->uri);
	pxfmstate->relation = relation;
	pxfmstate->options = options;
#if PG_VERSION_NUM < 90600
	pxfmstate->values = (Datum *) palloc(tupDesc->natts * sizeof(Datum));
	pxfmstate->nulls = (bool *) palloc(tupDesc->natts * sizeof(bool));
#endif

	InitCopyStateForModify(pxfmstate);

	resultRelInfo->ri_FdwState = pxfmstate;

	elog(DEBUG5, "pxf_fdw: pxfBeginForeignModify ends on segment: %d", PXF_SEGMENT_ID);
}

/*
 * pxfExecForeignInsert
 *		Insert one row into a foreign table
 */
static TupleTableSlot *
pxfExecForeignInsert(EState *estate,
					 ResultRelInfo *resultRelInfo,
					 TupleTableSlot *slot,
					 TupleTableSlot *planSlot)
{
	elog(DEBUG5, "pxf_fdw: pxfExecForeignInsert starts on segment: %d", PXF_SEGMENT_ID);

	PxfFdwModifyState *pxfmstate = (PxfFdwModifyState *) resultRelInfo->ri_FdwState;

	/* If pxfmstate is NULL, we are in MASTER when exec_location is all segments; nothing to do */
	if (pxfmstate == NULL)
		return NULL;

	CopyState	cstate = pxfmstate->cstate;
#if PG_VERSION_NUM < 90600
	Relation	relation = resultRelInfo->ri_RelationDesc;
	TupleDesc	tupDesc = RelationGetDescr(relation);
	HeapTuple	tuple = ExecMaterializeSlot(slot);
	Datum	   *values = pxfmstate->values;
	bool	   *nulls = pxfmstate->nulls;

	heap_deform_tuple(tuple, tupDesc, values, nulls);
	CopyOneRowTo(cstate, HeapTupleGetOid(tuple), values, nulls);
#else

	/* TEXT or CSV */
	slot_getallattrs(slot);
	CopyOneRowTo(cstate, slot);
#endif
	CopySendEndOfRow(cstate);

	StringInfo	fe_msgbuf = cstate->fe_msgbuf;

	int			bytes_written = PxfBridgeWrite(pxfmstate, fe_msgbuf->data, fe_msgbuf->len);

	if (bytes_written == -1)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to foreign resource: %m")));
	}

	elog(DEBUG3, "pxf_fdw %d bytes written", bytes_written);

	/* Reset our buffer to start clean next round */
	cstate->fe_msgbuf->len = 0;
	cstate->fe_msgbuf->data[0] = '\0';

	elog(DEBUG5, "pxf_fdw: pxfExecForeignInsert ends on segment: %d", PXF_SEGMENT_ID);
	return slot;
}

/*
 * pxfEndForeignModify
 *		Finish an insert/update/delete operation on a foreign table
 */
static void
pxfEndForeignModify(EState *estate,
					ResultRelInfo *resultRelInfo)
{
	elog(DEBUG5, "pxf_fdw: pxfEndForeignModify starts on segment: %d", PXF_SEGMENT_ID);

	PxfFdwModifyState *pxfmstate = (PxfFdwModifyState *) resultRelInfo->ri_FdwState;

	/* If pxfmstate is NULL, we are in EXPLAIN or MASTER when exec_location is all segments; nothing to do */
	if (pxfmstate == NULL)
		return;

	EndCopyFrom(pxfmstate->cstate);
	pxfmstate->cstate = NULL;
	PxfBridgeCleanup(pxfmstate);

	elog(DEBUG5, "pxf_fdw: pxfEndForeignModify ends on segment: %d", PXF_SEGMENT_ID);
}

/*
 * pxfIsForeignRelUpdatable
 *  Assume table is updatable regardless of settings.
 *		Determine whether a foreign table supports INSERT, UPDATE and/or
 *		DELETE.
 */
static int
pxfIsForeignRelUpdatable(Relation rel)
{
	elog(DEBUG5, "pxf_fdw: pxfIsForeignRelUpdatable starts on segment: %d", PXF_SEGMENT_ID);
	elog(DEBUG5, "pxf_fdw: pxfIsForeignRelUpdatable ends on segment: %d", PXF_SEGMENT_ID);
	/* Only INSERTs are allowed at the moment */
	return 1u << (unsigned int) CMD_INSERT | 0u << (unsigned int) CMD_UPDATE | 0u << (unsigned int) CMD_DELETE;
}

/*
 * Initiates a copy state for pxfBeginForeignScan() and pxfReScanForeignScan()
 */
static void
InitCopyState(PxfFdwScanState *pxfsstate)
{
	CopyState	cstate;

	PxfBridgeImportStart(pxfsstate);

	/*
	 * Create CopyState from FDW options.  We always acquire all columns, so
	 * as to match the expected ScanTupleSlot signature.
	 */
	cstate = BeginCopyFrom(
#if PG_VERSION_NUM >= 90600
						   NULL,
#endif
						   pxfsstate->relation,
						   NULL,
						   false,	/* is_program */
						   &PxfBridgeRead,	/* data_source_cb */
						   pxfsstate,	/* data_source_cb_extra */
						   NIL, /* attnamelist */
						   pxfsstate->options->copy_options	/* copy options */
#if PG_VERSION_NUM < 90600
						   ,NIL	/* ao_segnos */
#endif
						   );


	if (pxfsstate->options->reject_limit == -1)
	{
		/* Default error handling - "all-or-nothing" */
		cstate->cdbsreh = NULL; /* no SREH */
		cstate->errMode = ALL_OR_NOTHING;
	}
	else
	{
		/* no error log by default */
		cstate->errMode = SREH_IGNORE;

		/* select the SREH mode */
		if (pxfsstate->options->log_errors)
			cstate->errMode = SREH_LOG; /* errors into file */

		cstate->cdbsreh = makeCdbSreh(pxfsstate->options->reject_limit,
									  pxfsstate->options->is_reject_limit_rows,
									  pxfsstate->options->resource,
									  (char *) cstate->cur_relname,
#if PG_VERSION_NUM >= 90600
									  pxfsstate->options->log_errors ? LOG_ERRORS_ENABLE : LOG_ERRORS_DISABLE);
#else
									  pxfsstate->options->log_errors);
#endif

		cstate->cdbsreh->relid = RelationGetRelid(pxfsstate->relation);
	}

	/* and 'fe_mgbuf' */
	cstate->fe_msgbuf = makeStringInfo();

	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype input or output routines, and should be faster than retail
	 * pfree's anyway.
	 */
	cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "PxfFdwMemCxt",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	pxfsstate->cstate = cstate;
}

/*
 * Initiates a copy state for pxfBeginForeignModify()
 */
static void
InitCopyStateForModify(PxfFdwModifyState *pxfmstate)
{
	List	   *copy_options;
	CopyState	cstate;

	copy_options = pxfmstate->options->copy_options;

	PxfBridgeExportStart(pxfmstate);

	/*
	 * Create CopyState from FDW options.  We always acquire all columns, so
	 * as to match the expected ScanTupleSlot signature.
	 */
	cstate = BeginCopyTo(pxfmstate->relation, copy_options);

	/* Initialize 'out_functions', like CopyTo() would. */

	TupleDesc	tupDesc = RelationGetDescr(pxfmstate->relation);
#if PG_VERSION_NUM >= 90600
	Form_pg_attribute attr = tupDesc->attrs;
#else
	Form_pg_attribute *attr = tupDesc->attrs;
#endif
	int			num_phys_attrs = tupDesc->natts;

	cstate->out_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	ListCell   *cur;

	foreach(cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Oid			out_func_oid;
		bool		isvarlena;

#if PG_VERSION_NUM >= 90600
		getTypeOutputInfo(attr[attnum - 1].atttypid,
#else
		getTypeOutputInfo(attr[attnum - 1]->atttypid,
#endif
						  &out_func_oid,
						  &isvarlena);
		fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
	}

	/* and 'fe_mgbuf' */
	cstate->fe_msgbuf = makeStringInfo();

	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype input or output routines, and should be faster than retail
	 * pfree's anyway.
	 */
	cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "PxfFdwMemCxt",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	pxfmstate->cstate = cstate;
}

/*
 * Set up CopyState for writing to a foreign table.
 */
static CopyState
BeginCopyTo(Relation forrel, List *options)
{
	CopyState	cstate;

	Assert(forrel->rd_rel->relkind == RELKIND_FOREIGN_TABLE);

	cstate = BeginCopyToForeignTable(forrel, options);
	cstate->dispatch_mode = COPY_DIRECT;

	/*
	 * We use COPY_CALLBACK to mean that the each line should be left in
	 * fe_msgbuf. There is no actual callback!
	 */
	cstate->copy_dest = COPY_CALLBACK;

	/*
	 * Some more initialization, that in the normal COPY TO codepath, is done
	 * in CopyTo() itself.
	 */
	cstate->null_print_client = cstate->null_print; /* default */
	if (cstate->need_transcoding)
		cstate->null_print_client = pg_server_to_custom(cstate->null_print,
														cstate->null_print_len,
														cstate->file_encoding,
														cstate->enc_conversion_proc);

	return cstate;
}

/*
 * PXF specific error context callback for "begin foreign scan" operation.
 * It replaces the "COPY" term in the error message context with
 * the "Foreign table" term and provides the name of the foreign table
 */
static void
PxfBeginScanErrorCallback(void *arg) {
    Relation relation = (Relation) arg;
    if (relation) {
        errcontext("Foreign table %s", RelationGetRelationName(relation));
        return;
    }
}

/*
 * PXF specific error context callback for "iterate foreign scan" operation.
 * It is copied from copy.c handler for COPY FROM operation and replaces
 * the "COPY" term in the error message context with the "Foreign table" term.
 * It also replaces the error message form COPY stack with the one sent by PXF
 * if it detects that PXF provided the error token in an extra column of an error row.
 *
 * The argument for the error context must be PxfFdwScanState.
 */
void
PxfCopyFromErrorCallback(void *arg)
{
    PxfFdwScanState *pxfsstate = (PxfFdwScanState *) arg;
    CopyState	cstate = pxfsstate->cstate;
    char		curlineno_str[32];

    snprintf(curlineno_str, sizeof(curlineno_str), UINT64_FORMAT,
             cstate->cur_lineno);

    if (cstate->binary)
    {
        /* can't usefully display the data */
        if (cstate->cur_attname)
            errcontext("Foreign table %s, record %s of %s, column %s",
                       cstate->cur_relname, curlineno_str, pxfsstate->options->resource,
                       cstate->cur_attname);
        else
            errcontext("Foreign table %s, record %s of %s",
                       cstate->cur_relname, curlineno_str, pxfsstate->options->resource);
    }
    else
    {
        if (cstate->cur_attname && cstate->cur_attval)
        {
            /* error is relevant to a particular column */
            char	   *attval;

            attval = limit_printout_length(cstate->cur_attval);
            errcontext("Foreign table %s, record %s of %s, column %s: \"%s\"",
                       cstate->cur_relname, curlineno_str, pxfsstate->options->resource,
                       cstate->cur_attname, attval);
            pfree(attval);
        }
        else if (cstate->cur_attname)
        {
            /* error is relevant to a particular column, value is NULL */
            errcontext("Foreign table %s, record %s of %s, column %s: null input",
                       cstate->cur_relname, curlineno_str, pxfsstate->options->resource,
                       cstate->cur_attname);
        }
        else
        {
            /*
             * PXF specific addition: for CSV transfer PXF communicates an error
             * during processing by emitting an "error" line that has no values
             * for all columns and has an additional column that contains a marker
             * token and an actual error message in the format
             * ",, ... ,,PXFERRMSG> <actual message>"
             * The copy code fails to parse it with the message
             * "extra data after last expected column" that we want to change to
             * contain the actual error message reported by PXF
             */
            char *token_index = strstr(cstate->line_buf.data, PXF_ERROR_TOKEN);
            if (token_index != NULL) {
                /* token was found, get the actual message and set it as the main error message */
                errmsg("%s", token_index + PXF_ERROR_TOKEN_SIZE);
                errcontext("Foreign table %s, record %s of %s",
                           cstate->cur_relname, curlineno_str, pxfsstate->options->resource);
            }
            /*
             * Error is relevant to a particular line.
             *
             * If line_buf still contains the correct line, and it's already
             * transcoded, print it. If it's still in a foreign encoding, it's
             * quite likely that the error is precisely a failure to do
             * encoding conversion (ie, bad data). We dare not try to convert
             * it, and at present there's no way to regurgitate it without
             * conversion. So we have to punt and just report the line number.
             */
            else if (cstate->line_buf_valid &&
                (cstate->line_buf_converted || !cstate->need_transcoding))
            {
                char	   *lineval;

                lineval = limit_printout_length(cstate->line_buf.data);
                //truncateEolStr(line_buf, cstate->eol_type); <-- this is done in GP6, but not in GP7 ?
                errcontext("Foreign table %s, record %s of %s: \"%s\"",
                           cstate->cur_relname, curlineno_str, pxfsstate->options->resource, lineval);
                pfree(lineval);
            }
            else
            {
                /*
                 * Here, the line buffer is still in a foreign encoding,
                 * and indeed it's quite likely that the error is precisely
                 * a failure to do encoding conversion (ie, bad data).	We
                 * dare not try to convert it, and at present there's no way
                 * to regurgitate it without conversion.  So we have to punt
                 * and just report the line number.
                 */
                errcontext("Foreign table %s, record %s of %s",
                           cstate->cur_relname, curlineno_str, pxfsstate->options->resource);
            }
        }
    }
}
