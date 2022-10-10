/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "pxffilters.h"
#include "pxfheaders.h"
#include "commands/defrem.h"
#if PG_VERSION_NUM >= 120000
#include "access/external.h"
#include "extension/gp_exttable_fdw/extaccess.h"
#include "executor/execExpr.h"
#else
#include "access/fileam.h"
#include "catalog/pg_exttable.h"
#endif
#include "utils/timestamp.h"
#include "nodes/makefuncs.h"
#include "cdb/cdbvars.h"



/* helper function declarations */
static void add_alignment_size_httpheader(CHURL_HEADERS headers);
static void add_tuple_desc_httpheader(CHURL_HEADERS headers, Relation rel);
static void add_location_options_httpheader(CHURL_HEADERS headers, GPHDUri *gphduri);
static char *get_format_name(char fmtcode);

#if PG_VERSION_NUM >= 120000
static void add_projection_desc_httpheader_pg12(CHURL_HEADERS headers, ProjectionInfo *projInfo, List *qualsAttributes, Relation rel);
#else
static void add_projection_desc_httpheader_pg94(CHURL_HEADERS headers, ProjectionInfo *projInfo, List *qualsAttributes, Relation rel);
#endif

static bool add_attnums_from_targetList(Node *node, List *attnums);
static void add_projection_index_header(CHURL_HEADERS pVoid, StringInfoData data, int attno, char number[32]);
#if PG_VERSION_NUM < 90400
static List *parseCopyFormatString(Relation rel, char *fmtstr, char fmttype);
static List *appendCopyEncodingOption(List *copyFmtOpts, int encoding);
#endif

/*
 * Add key/value pairs to connection header.
 * These values are the context of the query and used
 * by the remote component.
 */
void
build_http_headers(PxfInputData *input)
{
	extvar_t       ev;
	CHURL_HEADERS  headers    = input->headers;
	GPHDUri        *gphduri   = input->gphduri;
	Relation       rel        = input->rel;
	char           *filterstr = input->filterstr;
	char           *data_encoding = NULL;
	char           long_number[sizeof(int32) * 8];
	ProjectionInfo *proj_info = input->proj_info;
	const char	   *relname;
	char		   *relnamespace = NULL;

	relname = gphduri->data;
	if (rel != NULL)
	{
		/* format */
		ExtTableEntry *exttbl = GetExtTableEntry(rel->rd_id);
		ListCell   *option;
		List	   *copyFmtOpts = NIL;

		/* pxf treats CSV as TEXT */
		char *format = get_format_name(exttbl->fmtcode);

		churl_headers_append(headers, "X-GP-FORMAT", format);

		/* Parse fmtOptString here */
		if (fmttype_is_text(exttbl->fmtcode) || fmttype_is_csv(exttbl->fmtcode))
		{
#if PG_VERSION_NUM >= 120000
			copyFmtOpts = exttbl->options;
#else
			copyFmtOpts = parseCopyFormatString(rel, exttbl->fmtopts, exttbl->fmtcode);
#endif
		}

#if PG_VERSION_NUM >= 120000
		/* pass external table's encoding to copy's options */
		copyFmtOpts = lappend(copyFmtOpts, makeDefElem("encoding", (Node *)makeString((char *)pg_encoding_to_char(exttbl->encoding)), -1));
#else
        copyFmtOpts = appendCopyEncodingOption(copyFmtOpts, exttbl->encoding);
#endif

		/* Extract options from the statement node tree */
		foreach(option, copyFmtOpts)
		{
			DefElem    *def = (DefElem *) lfirst(option);

			if (strcmp(def->defname, "encoding") == 0)
			{
				data_encoding = defGetString(def);
			}
			else
			{
				churl_headers_append(headers, normalize_key_name(def->defname), defGetString(def));
			}
		}

		/* Record fields - name and type of each field */
		add_tuple_desc_httpheader(headers, rel);

		relname = RelationGetRelationName(rel);
		relnamespace = GetNamespaceName(RelationGetNamespace(rel));
	}

	if (proj_info != NULL)
	{
		bool qualsAreSupported = true;
		List *qualsAttributes =
				 extractPxfAttributes(input->quals, &qualsAreSupported);
		/* projection information is incomplete if columns from WHERE clause wasn't extracted */
		/* if any of expressions in WHERE clause is not supported - do not send any projection information at all*/
		if (qualsAreSupported &&
			(qualsAttributes != NIL || list_length(input->quals) == 0))
		{
		   #if PG_VERSION_NUM >= 120000
		        add_projection_desc_httpheader_pg12(headers, proj_info, qualsAttributes, rel);
		   #else
			    add_projection_desc_httpheader_pg94(headers, proj_info, qualsAttributes, rel);
		   #endif
		}
		else
		{
			elog(DEBUG2,
				 "Query will not be optimized to use projection information");
		}
	}

	/* GP cluster configuration */
	external_set_env_vars(&ev, gphduri->uri, false, NULL, NULL, false, 0);

	/* make sure that user identity is known and set, otherwise impersonation by PXF will be impossible */
	if (!ev.GP_USER || !ev.GP_USER[0])
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("user identity is unknown")));
	churl_headers_append(headers, "X-GP-ENCODED-HEADER-VALUES", "true");
	churl_headers_append(headers, "X-GP-USER", ev.GP_USER);

	churl_headers_append(headers, "X-GP-SEGMENT-ID", ev.GP_SEGMENT_ID);
	churl_headers_append(headers, "X-GP-SEGMENT-COUNT", ev.GP_SEGMENT_COUNT);
	churl_headers_append(headers, "X-GP-XID", ev.GP_XID);
	churl_headers_append(headers, "X-GP-PXF-API-VERSION", PXF_API_VERSION);

	pg_ltoa(gp_session_id, long_number);
	churl_headers_append(headers, "X-GP-SESSION-ID", long_number);
	pg_ltoa(gp_command_count, long_number);
	churl_headers_append(headers, "X-GP-COMMAND-COUNT", long_number);

	add_alignment_size_httpheader(headers);

	/* headers for uri data */
	churl_headers_append(headers, "X-GP-URL-HOST", gphduri->host);
	churl_headers_append(headers, "X-GP-URL-PORT", gphduri->port);
	churl_headers_append(headers, "X-GP-DATA-DIR", gphduri->data);
	churl_headers_append(headers, "X-GP-TABLE-NAME", relname);
	churl_headers_append(headers, "X-GP-SCHEMA-NAME", relnamespace);

	/* encoding options */
	churl_headers_append(headers, "X-GP-DATA-ENCODING", data_encoding);
	churl_headers_append(headers, "X-GP-DATABASE-ENCODING", GetDatabaseEncodingName());

	/* location options */
	add_location_options_httpheader(headers, gphduri);

	/* full uri */
	churl_headers_append(headers, "X-GP-URI", gphduri->uri);

	/* filters */
	if (filterstr != NULL)
	{
		churl_headers_append(headers, "X-GP-FILTER", filterstr);
		churl_headers_append(headers, "X-GP-HAS-FILTER", "1");
	}
	else
		churl_headers_append(headers, "X-GP-HAS-FILTER", "0");

	// Since we only establish a single connection per segment, we can safely close the connection after
	// the segment completes streaming data.
	churl_headers_override(headers, "Connection", "close");
}

/* Report alignment size to remote component
 * GPDBWritable uses alignment that has to be the same as
 * in the C code.
 * Since the C code can be compiled for both 32 and 64 bits,
 * the alignment can be either 4 or 8.
 */
static void
add_alignment_size_httpheader(CHURL_HEADERS headers)
{
	char		tmp[sizeof(char *)];

	pg_ltoa(sizeof(char *), tmp);
	churl_headers_append(headers, "X-GP-ALIGNMENT", tmp);
}

/*
 * Report tuple description to remote component
 * Currently, number of attributes, attributes names, types and types modifiers
 * Each attribute has a pair of key/value
 * where X is the number of the attribute
 * X-GP-ATTR-NAMEX - attribute X's name
 * X-GP-ATTR-TYPECODEX - attribute X's type OID (e.g, 16)
 * X-GP-ATTR-TYPENAMEX - attribute X's type name (e.g, "boolean")
 * optional - X-GP-ATTR-TYPEMODX-COUNT - total number of modifier for attribute X
 * optional - X-GP-ATTR-TYPEMODX-Y - attribute X's modifiers Y (types which have precision info, like numeric(p,s))
 *
 * If a column has been dropped from the external table definition, that
 * column will not be reported to the PXF server (as if it never existed).
 * For example:
 *
 *  ---------------------------------------------
 * |  col1  |  col2  |  col3 (dropped)  |  col4  |
 *  ---------------------------------------------
 *
 * Col4 will appear as col3 to the PXF server as if col3 never existed, and
 * only 3 columns will be reported to PXF server.
 */
static void
add_tuple_desc_httpheader(CHURL_HEADERS headers, Relation rel)
{
	int				i, attrIx;
	char			long_number[sizeof(int32) * 8];
	StringInfoData	formatter;
	TupleDesc		tuple;

	initStringInfo(&formatter);

	/* Get tuple description itself */
	tuple = RelationGetDescr(rel);

	/* Iterate attributes */
	for (i = 0, attrIx = 0; i < tuple->natts; ++i)
	{
        #if PG_VERSION_NUM >= 120000
            FormData_pg_attribute *attribute = &tuple->attrs[i];
        #else
            FormData_pg_attribute *attribute = tuple->attrs[i];
        #endif

		/* Ignore dropped attributes. */
		if (attribute->attisdropped)
			continue;

		/* Add a key/value pair for attribute name */
		resetStringInfo(&formatter);
		appendStringInfo(&formatter, "X-GP-ATTR-NAME%u", attrIx);
		churl_headers_append(headers, formatter.data, attribute->attname.data);

		/* Add a key/value pair for attribute type */
		resetStringInfo(&formatter);
		appendStringInfo(&formatter, "X-GP-ATTR-TYPECODE%u", attrIx);
		pg_ltoa(attribute->atttypid, long_number);
		churl_headers_append(headers, formatter.data, long_number);

		/* Add a key/value pair for attribute type name */
		resetStringInfo(&formatter);
		appendStringInfo(&formatter, "X-GP-ATTR-TYPENAME%u", attrIx);
		churl_headers_append(headers, formatter.data, TypeOidGetTypename(attribute->atttypid));

		/* Add attribute type modifiers if any */
		if (attribute->atttypmod > -1)
		{
			switch (attribute->atttypid)
			{
				case NUMERICOID:
				case NUMERIC_ARRAY_OID:
					{
						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-COUNT", attrIx);
						pg_ltoa(2, long_number);
						churl_headers_append(headers, formatter.data, long_number);


						/* precision */
						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-%u", attrIx, 0);
						pg_ltoa((attribute->atttypmod >> 16) & 0xffff, long_number);
						churl_headers_append(headers, formatter.data, long_number);

						/* scale */
						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-%u", attrIx, 1);
						pg_ltoa((attribute->atttypmod - VARHDRSZ) & 0xffff, long_number);
						churl_headers_append(headers, formatter.data, long_number);
						break;
					}
				case CHAROID:
				case CHAR_ARRAY_OID:
				case BPCHAROID:
				case BPCHAR_ARRAY_OID:
				case VARCHAROID:
				case VARCHAR_ARRAY_OID:
					{
						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-COUNT", attrIx);
						pg_ltoa(1, long_number);
						churl_headers_append(headers, formatter.data, long_number);

						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-%u", attrIx, 0);
						pg_ltoa((attribute->atttypmod - VARHDRSZ), long_number);
						churl_headers_append(headers, formatter.data, long_number);
						break;
					}
				case VARBITOID:
				case VARBIT_ARRAY_OID:
				case BITOID:
				case BIT_ARRAY_OID:
				case TIMESTAMPOID:
				case TIMESTAMP_ARRAY_OID:
				case TIMESTAMPTZOID:
				case TIMESTAMPTZ_ARRAY_OID:
				case TIMEOID:
				case TIME_ARRAY_OID:
				case TIMETZOID:
				case TIMETZ_ARRAY_OID:
					{
						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-COUNT", attrIx);
						pg_ltoa(1, long_number);
						churl_headers_append(headers, formatter.data, long_number);

						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-%u", attrIx, 0);
						pg_ltoa((attribute->atttypmod), long_number);
						churl_headers_append(headers, formatter.data, long_number);
						break;
					}
				case INTERVALOID:
				case INTERVAL_ARRAY_OID:
					{
						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-COUNT", attrIx);
						pg_ltoa(1, long_number);
						churl_headers_append(headers, formatter.data, long_number);

						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-%u", attrIx, 0);
						pg_ltoa(INTERVAL_PRECISION(attribute->atttypmod), long_number);
						churl_headers_append(headers, formatter.data, long_number);
						break;
					}
				default:
					elog(DEBUG5, "add_tuple_desc_httpheader: unsupported type %d ", attribute->atttypid);
					break;
			}
		}
		attrIx++;
	}

	/* Convert the number of attributes to a string */
	pg_ltoa(attrIx, long_number);
	churl_headers_append(headers, "X-GP-ATTRS", long_number);

	pfree(formatter.data);
}

/*
 * Report projection description to the remote component, the indices of
 * dropped columns do not get reported, as if they never existed, and
 * column indices that follow dropped columns will be shifted by the number
 * of dropped columns that precede it. For example,
 *
 *  ---------------------------------------------
 * |  col1  |  col2 (dropped)  |  col3  |  col4  |
 *  ---------------------------------------------
 *
 * Let's assume that col1 and col4 are projected, the reported projected
 * indices will be 0, 2. This is because we use 0-based indexing and because
 * col2 was dropped, the indices for col3 and col4 get shifted by -1.
 */

#if PG_VERSION_NUM < 120000
static void
add_projection_desc_httpheader_pg94(CHURL_HEADERS headers,
							   ProjectionInfo *projInfo,
							   List *qualsAttributes,
							   Relation rel)
{
	int			   i;
	int			   dropped_count;
	int			   number;
#if PG_VERSION_NUM < 90400
	int			   numSimpleVars;
	int			   numTargetList;
#if PG_VERSION_NUM < 90400
	int			   numSimpleVars;
#endif
	char			long_number[sizeof(int32) * 8];
	int				*varNumbers = projInfo->pi_varNumbers;
	Bitmapset		*attrs_used;
	StringInfoData	formatter;
	TupleDesc		tupdesc;

	initStringInfo(&formatter);
	attrs_used = NULL;
	number = 0;

	numTargetList = 0;

#if PG_VERSION_NUM >= 90400
	/*
	 * Non-simpleVars are added to the targetlist
	 * we use expression_tree_walker to access attrno information
	 * we do it through a helper function add_attnums_from_targetList
	 */
	if (projInfo->pi_targetlist)
	{
#else
	numSimpleVars = 0;

	if (!varNumbers)
	{
		/*
		 * When there are not just simple Vars we need to
		 * walk the tree to get attnums
		 */
#endif
		List     *l = lappend_int(NIL, 0);
		ListCell *lc1;

		foreach(lc1, projInfo->pi_targetlist)
		{
			GenericExprState *gstate = (GenericExprState *) lfirst(lc1);
			add_attnums_from_targetList((Node *) gstate->arg->expr, l);
		}

		foreach(lc1, l)
		{
			int attno = lfirst_int(lc1);
			if (attno > InvalidAttrNumber)
			{
				attrs_used =
					bms_add_member(attrs_used,
									attno - FirstLowInvalidHeapAttributeNumber);
			}
		}

		list_free(l);
	}
#if PG_VERSION_NUM < 90400
	else
	{
		numSimpleVars = list_length(projInfo->pi_targetlist);
	}
#endif

	number = numTargetList +
#if PG_VERSION_NUM >= 90400
		projInfo->pi_numSimpleVars +
#else
		numSimpleVars +
#endif
		list_length(qualsAttributes);
	if (number == 0)
		return;

	attrs_used = NULL;

	/* Convert the number of projection columns to a string */
	pg_ltoa(number, long_number);
	churl_headers_append(headers, "X-GP-ATTRS-PROJ", long_number);

#if PG_VERSION_NUM >= 90400
	for (i = 0; i < projInfo->pi_numSimpleVars; i++)
#else
	for (i = 0; varNumbers && i < numSimpleVars; i++)
#endif
	{
		attrs_used =
			bms_add_member(attrs_used,
						 varNumbers[i] - FirstLowInvalidHeapAttributeNumber);
	}

	ListCell *attribute = NULL;

	/*
	 * AttrNumbers coming from quals
	 */
	foreach(attribute, qualsAttributes)
	{
		AttrNumber attrNumber = (AttrNumber) lfirst_int(attribute);
		attrs_used =
			bms_add_member(attrs_used,
						 attrNumber + 1 - FirstLowInvalidHeapAttributeNumber);
	}

	tupdesc = RelationGetDescr(rel);
	dropped_count = 0;

	for (i = 1; i <= tupdesc->natts; i++)
	{
		/* Ignore dropped attributes. */
		if (tupdesc->attrs[i - 1]->attisdropped)
		{
			/* keep a counter of the number of dropped attributes */
			dropped_count++;
			continue;
		}

		if (bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used))
		{
			/* Shift the column index by the running dropped_count */
			add_projection_index_header(headers, formatter,
										i - 1 - dropped_count, long_number);
		}
	}

	list_free(qualsAttributes);
	pfree(formatter.data);
	bms_free(attrs_used);
}
#endif

/*
 * Report projection description to the remote component, the indices of
 * dropped columns do not get reported, as if they never existed, and
 * column indices that follow dropped columns will be shifted by the number
 * of dropped columns that precede it. For example,
 *
 *  ---------------------------------------------
 * |  col1  |  col2 (dropped)  |  col3  |  col4  |
 *  ---------------------------------------------
 *
 * Let's assume that col1 and col4 are projected, the reported projected
 * indices will be 0, 2. This is because we use 0-based indexing and because
 * col2 was dropped, the indices for col3 and col4 get shifted by -1.
 */

#if PG_VERSION_NUM >= 120000
static void
add_projection_desc_httpheader_pg12(CHURL_HEADERS headers,
							   ProjectionInfo *projInfo,
							   List *qualsAttributes,
							   Relation rel)
{
	int			   i;
	int			   dropped_count;
	int			   number;
	int			   numTargetList;
	char			long_number[sizeof(int32) * 8];
    // In versions < 120000, projInfo->pi_varNumbers contains atttribute numbers of SimpleVars
    // Since,this pi_varNumbers doesn't exist in PG12 and above, we can add the attribute numbers by
    // iterating on the Simple vars.
	int				varNumbers[sizeof(int32) * 8];
	Bitmapset		*attrs_used;
	StringInfoData	formatter;
	TupleDesc		tupdesc;
	initStringInfo(&formatter);
	numTargetList = 0;

	 List *targetList = (List *) projInfo->pi_state.expr;
     int  numSimpleVars = 0;

	int numNonSimpleVars = 0;

    // TODO Re-evaluate this logic and may be we don't need numNonSimpleVars
	 for (int i = 0; i < projInfo->pi_state.steps_len; i++)
     {
         ExprEvalStep *step = &projInfo->pi_state.steps[i];
         ExprEvalOp	opcode = ExecEvalStepOp(&projInfo->pi_state, step);
             if ( opcode == EEOP_ASSIGN_INNER_VAR ||
                     opcode == EEOP_ASSIGN_OUTER_VAR ||
                     opcode == EEOP_ASSIGN_SCAN_VAR)
                     numSimpleVars++;
             else if (opcode == EEOP_ASSIGN_TMP_MAKE_RO ||
                        opcode == EEOP_ASSIGN_TMP)
                     numNonSimpleVars++;
     }

	/*
	 * Non-simpleVars are added to the targetlist
	 * we use expression_tree_walker to access attrno information
	 * we do it through a helper function add_attnums_from_targetList
	 */
	if (targetList)
	{

		List     *l = lappend_int(NIL, 0);
		ListCell *lc1;

		foreach(lc1, targetList)
		{
			ExprState *gstate = (ExprState *) lfirst(lc1);
            add_attnums_from_targetList( (Node *) gstate, l);
		}

        int i=0;
		foreach(lc1, l)
		{
			int attno = lfirst_int(lc1);
			if (attno > InvalidAttrNumber)
			{
				add_projection_index_header(headers,
											formatter, attno - 1, long_number);
				numTargetList++;
                varNumbers[i] = attno;
                i++;
			}
		}

		list_free(l);
	}

	number = numTargetList + numSimpleVars + list_length(qualsAttributes);
	if (number == 0)
		return;

	attrs_used = NULL;

	/* Convert the number of projection columns to a string */
	pg_ltoa(number, long_number);
	churl_headers_append(headers, "X-GP-ATTRS-PROJ", long_number);

    for (i = 0;varNumbers && i < numSimpleVars ; i++)
	{
		attrs_used =
			bms_add_member(attrs_used,
                           varNumbers[i] - FirstLowInvalidHeapAttributeNumber);
	}

	ListCell *attribute = NULL;

	/*
	 * AttrNumbers coming from quals
	 */
	foreach(attribute, qualsAttributes)
	{
		AttrNumber attrNumber = (AttrNumber) lfirst_int(attribute);
		attrs_used =
			bms_add_member(attrs_used,
						 attrNumber + 1 - FirstLowInvalidHeapAttributeNumber);
	}

	tupdesc = RelationGetDescr(rel);
	dropped_count = 0;

	for (i = 1; i <= tupdesc->natts; i++)
	{
		/* Ignore dropped attributes. */
		if (tupdesc->attrs[i - 1].attisdropped)
		{
			/* keep a counter of the number of dropped attributes */
			dropped_count++;
			continue;
		}

		if (bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used))
		{
			/* Shift the column index by the running dropped_count */
			add_projection_index_header(headers, formatter,
										i - 1 - dropped_count, long_number);
			number++;
		}
	}

	if (number != 0)
	{
		/* Convert the number of projection columns to a string */
		pg_ltoa(number, long_number);
		churl_headers_append(headers, "X-GP-ATTRS-PROJ", long_number);
	}

	list_free(qualsAttributes);
	pfree(formatter.data);
	bms_free(attrs_used);
}
#endif


/*
 * Adds the projection index header for the given attno
 */
static void
add_projection_index_header(CHURL_HEADERS headers,
							StringInfoData str,
							int attno,
							char long_number[32])
{
	pg_ltoa(attno, long_number);
	resetStringInfo(&str);
	appendStringInfo(&str, "X-GP-ATTRS-PROJ-IDX");
	churl_headers_append(headers, str.data, long_number);
}

/*
 * The options in the LOCATION statement of "create external table"
 * FRAGMENTER=HdfsDataFragmenter&ACCESSOR=SequenceFileAccessor...
 */
static void
add_location_options_httpheader(CHURL_HEADERS headers, GPHDUri *gphduri)
{
	ListCell   *option = NULL;

	foreach(option, gphduri->options)
	{
		OptionData *data = (OptionData *) lfirst(option);
		char	   *x_gp_key = normalize_key_name(data->key);

		churl_headers_append(headers, x_gp_key, data->value);
		pfree(x_gp_key);
	}
}

/*
 * Converts a character code for the format name into a string of format definition
 */
static char *
get_format_name(char fmtcode)
{
	char	   *formatName = NULL;

	if (fmttype_is_text(fmtcode) || fmttype_is_csv(fmtcode))
	{
		formatName = TextFormatName;
	}
	else if (fmttype_is_custom(fmtcode))
	{
		formatName = GpdbWritableFormatName;
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unable to get format name for format code: %c",
						fmtcode)));
	}

	return formatName;
}

/*
 * Gets a list of attnums from the given Node
 * it uses expression_tree_walker to recursively
 * get the list
 */
static bool
add_attnums_from_targetList(Node *node, List *attnums)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var        *variable = (Var *) node;
		AttrNumber attnum    = variable->varattno;

		lappend_int(attnums, attnum);
		return false;
	}

	/*
	 * Don't examine the arguments or filters of Aggrefs or WindowFunc/WindowRef,
	 * because those do not represent expressions to be evaluated within the
	 * overall targetlist's econtext.
	 */
	if (IsA(node, Aggref))
		return false;
#if PG_VERSION_NUM >= 90400
	if (IsA(node, WindowFunc))
#else
	if (IsA(node, WindowRef))
#endif
		return false;
	return expression_tree_walker(node,
								  add_attnums_from_targetList,
								  (void *) attnums);
}

#if PG_VERSION_NUM < 90400
/*
 * This function is copied from fileam.c in the 6X_STABLE branch.
 * In version 6, this function is no longer required to be copied.
 */
static List *
parseCopyFormatString(Relation rel, char *fmtstr, char fmttype)
{
	char	   *token;
	const char *whitespace = " \t\n\r";
	char		nonstd_backslash = 0;
	int			encoding = GetDatabaseEncoding();
	List	   *l = NIL;

	token = strtokx2(fmtstr, whitespace, NULL, NULL,
	                 0, false, true, encoding);

	while (token)
	{
		bool		fetch_next;
		DefElem	   *item = NULL;

		fetch_next = true;

		if (pg_strcasecmp(token, "header") == 0)
		{
			item = makeDefElem("header", (Node *)makeInteger(TRUE));
		}
		else if (pg_strcasecmp(token, "delimiter") == 0)
		{
			token = strtokx2(NULL, whitespace, NULL, "'",
			                 nonstd_backslash, true, true, encoding);
			if (!token)
				goto error;

			item = makeDefElem("delimiter", (Node *)makeString(pstrdup(token)));
		}
		else if (pg_strcasecmp(token, "null") == 0)
		{
			token = strtokx2(NULL, whitespace, NULL, "'",
			                 nonstd_backslash, true, true, encoding);
			if (!token)
				goto error;

			item = makeDefElem("null", (Node *)makeString(pstrdup(token)));
		}
		else if (pg_strcasecmp(token, "quote") == 0)
		{
			token = strtokx2(NULL, whitespace, NULL, "'",
			                 nonstd_backslash, true, true, encoding);
			if (!token)
				goto error;

			item = makeDefElem("quote", (Node *)makeString(pstrdup(token)));
		}
		else if (pg_strcasecmp(token, "escape") == 0)
		{
			token = strtokx2(NULL, whitespace, NULL, "'",
			                 nonstd_backslash, true, true, encoding);
			if (!token)
				goto error;

			item = makeDefElem("escape", (Node *)makeString(pstrdup(token)));
		}
		else if (pg_strcasecmp(token, "force") == 0)
		{
			List	   *cols = NIL;

			token = strtokx2(NULL, whitespace, ",", "\"",
			                 0, false, false, encoding);
			if (pg_strcasecmp(token, "not") == 0)
			{
				token = strtokx2(NULL, whitespace, ",", "\"",
				                 0, false, false, encoding);
				if (pg_strcasecmp(token, "null") != 0)
					goto error;
				/* handle column list */
				fetch_next = false;
				for (;;)
				{
					token = strtokx2(NULL, whitespace, ",", "\"",
					                 0, false, false, encoding);
					if (!token || strchr(",", token[0]))
						goto error;

					cols = lappend(cols, makeString(pstrdup(token)));

					/* consume the comma if any */
					token = strtokx2(NULL, whitespace, ",", "\"",
					                 0, false, false, encoding);
					if (!token || token[0] != ',')
						break;
				}

				item = makeDefElem("force_not_null", (Node *)cols);
			}
			else if (pg_strcasecmp(token, "quote") == 0)
			{
				fetch_next = false;
				for (;;)
				{
					token = strtokx2(NULL, whitespace, ",", "\"",
					                 0, false, false, encoding);
					if (!token || strchr(",", token[0]))
						goto error;

					/*
					 * For a '*' token the format option is force_quote_all
					 * and we need to recreate the column list for the entire
					 * relation.
					 */
					if (strcmp(token, "*") == 0)
					{
						int			i;
						TupleDesc	tupdesc = RelationGetDescr(rel);

						for (i = 0; i < tupdesc->natts; i++)
						{
							Form_pg_attribute att = tupdesc->attrs[i];

							if (att->attisdropped)
								continue;

							cols = lappend(cols, makeString(NameStr(att->attname)));
						}

						/* consume the comma if any */
						token = strtokx2(NULL, whitespace, ",", "\"",
						                 0, false, false, encoding);
						break;
					}

					cols = lappend(cols, makeString(pstrdup(token)));

					/* consume the comma if any */
					token = strtokx2(NULL, whitespace, ",", "\"",
					                 0, false, false, encoding);
					if (!token || token[0] != ',')
						break;
				}

				item = makeDefElem("force_quote", (Node *)cols);
			}
			else
				goto error;
		}
		else if (pg_strcasecmp(token, "fill") == 0)
		{
			token = strtokx2(NULL, whitespace, ",", "\"",
			                 0, false, false, encoding);
			if (pg_strcasecmp(token, "missing") != 0)
				goto error;

			token = strtokx2(NULL, whitespace, ",", "\"",
			                 0, false, false, encoding);
			if (pg_strcasecmp(token, "fields") != 0)
				goto error;

			item = makeDefElem("fill_missing_fields", (Node *)makeInteger(TRUE));
		}
		else if (pg_strcasecmp(token, "newline") == 0)
		{
			token = strtokx2(NULL, whitespace, NULL, "'",
			                 nonstd_backslash, true, true, encoding);
			if (!token)
				goto error;

			item = makeDefElem("newline", (Node *)makeString(pstrdup(token)));
		}
		else
			goto error;

		if (item)
			l = lappend(l, item);

		if (fetch_next)
			token = strtokx2(NULL, whitespace, NULL, NULL,
			                 0, false, false, encoding);
	}

	if (fmttype_is_text(fmttype))
	{
		/* TEXT is the default */
	}
	else if (fmttype_is_csv(fmttype))
	{
		/* Add FORMAT 'CSV' option to the beginning of the list */
		l = lcons(makeDefElem("format", (Node *) makeString("csv")), l);
	}
	else
		elog(ERROR, "unrecognized format type '%c'", fmttype);

	return l;

	error:
	if (token)
		ereport(ERROR,
		        (errcode(ERRCODE_INTERNAL_ERROR),
			        errmsg("external table internal parse error at \"%s\"",
			               token)));
	else
		ereport(ERROR,
		        (errcode(ERRCODE_INTERNAL_ERROR),
			        errmsg("external table internal parse error at end of line")));
}

/*
 * This function is copied from fileam.c in the 6X_STABLE branch.
 * In version 6, this function is no longer required to be copied.
 */
static List *
appendCopyEncodingOption(List *copyFmtOpts, int encoding)
{
	return lappend(copyFmtOpts, makeDefElem("encoding", (Node *)makeString((char *)pg_encoding_to_char(encoding))));
}
#endif