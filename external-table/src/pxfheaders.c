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
static char *get_format_name(ExtTableEntry *exttbl);
static bool isFormatterPxfWritable(ExtTableEntry *exttbl);
static void add_projection_desc_httpheaders(CHURL_HEADERS headers, ProjectionInfo *projInfo, List *qualsAttributes, Relation rel);
static bool add_attnums_from_targetList(Node *node, List *attnums);
static void add_projection_index_header(CHURL_HEADERS pVoid, StringInfoData data, int attno, char number[32]);
static List *appendCopyEncodingOptionToList(List *copyFmtOpts, int encoding);
static int  *getVarNumbers(ProjectionInfo *projInfo);
static List *getTargetList(ProjectionInfo *projInfo);
static bool needToIterateTargetList(List *targetList, int *varNumbers);
static Node *getTargetListEntryExpression(ListCell *lc1);
static int  getNumSimpleVars(ProjectionInfo *projInfo);
static char *
strtokx2(const char *s,
         const char *whitespace,
         const char *delim,
         const char *quote,
         char escape,
         bool e_strings,
         bool del_quotes,
         int encoding);
#if PG_VERSION_NUM < 90400
/*
 * this function is copied from Greenplum 6 (6X_STABLE branch) code
 * it is defined here for compilation with Greenplum 5 only
 * for compilation with Greenplum 6 it is defined in included fileam.h
 * in Greenplum 7 we do not need to define and call it as all options (copy or custom)
 * are added to ExtTableEntry.options list by external.c::GetExtFromForeignTableOptions()
 */
static List *parseCopyFormatString(Relation rel, char *fmtstr, char fmttype);

// Copied this Macro from tupdesc.h (6.x), since this is not present in GPDB 5
/* Accessor for the i'th attribute of tupdesc. */
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])
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
    char           *formatter_name = NULL;
    List	       *formatter_params = NIL;
	relname = gphduri->data;
	if (rel != NULL)
	{
		/* format */
		ExtTableEntry *exttbl = GetExtTableEntry(rel->rd_id);
		ListCell   *option;
		List	   *copyFmtOpts = NIL;

		/* pxf treats everything but pxfwritable_[import|export] as TEXT (even CSV) */
		char *format = get_format_name(exttbl);

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

		/* pass external table's encoding to copy's options */
		copyFmtOpts = appendCopyEncodingOptionToList(copyFmtOpts, exttbl->encoding);

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
			add_projection_desc_httpheaders(headers, proj_info, qualsAttributes, rel);
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
		FormData_pg_attribute *attribute = TupleDescAttr(tuple, i);

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
 * Returns pre-computed array of simple var attrnos, if available
 */
static inline int*
getVarNumbers(ProjectionInfo *projInfo) {
#if PG_VERSION_NUM >= 120000
	return NULL; // does not exist in projInfo in GP7
#else
	return projInfo->pi_varNumbers;
#endif
}

/*
 * Returns targetList from provided ProjectionInfo
 */
static inline List*
getTargetList(ProjectionInfo *projInfo)
{
#if PG_VERSION_NUM >= 120000
	return (List *) projInfo->pi_state.expr;
#else
	return projInfo->pi_targetlist;
#endif
}

/*
 * Determines whether there is a need to iterate over the targetList to find projected attributes
 */
static inline bool
needToIterateTargetList(List *targetList, int *varNumbers)
{
#if PG_VERSION_NUM >= 90400
	/*
	 * In GP6 non-simple Vars are added to the targetlist of ProjectionInfo while
	 * simple Vars are pre-computed and their attnos are placed into varNumbers array
	 * In GP7 everything is in targetList
	 */
	return (targetList != NULL);
#else
	/*
	 * In GP5 if targetList contains ONLY simple Vars their attrnos will be populated into varNumbers array
	 * otherwise the varNumbers array will be NULL, and we will need to iterate over the targetList
	 */
	return (varNumbers == NULL);
#endif
}

/*
 * Returns expression for a targetList entry.
 */
static inline
Node *getTargetListEntryExpression(ListCell *lc1)
{
#if PG_VERSION_NUM >= 120000
	ExprState *gstate = (ExprState *) lfirst(lc1);
	return (Node *) gstate;
#else
	GenericExprState *gstate = (GenericExprState *) lfirst(lc1);
	return (Node *) gstate->arg->expr;
#endif
};

/*
 * Returns a count of simpleVars if they were pre-computed.
 */
static inline
int getNumSimpleVars(ProjectionInfo *projInfo)
{
	int numSimpleVars = 0;
#if PG_VERSION_NUM < 90400
	// in GP5 if varNumbers is not NULL, it means the attrnos have been pre-computed in varNumbers
	// and targetList consists only of simpleVars, so we can use its length
	if (projInfo->pi_varNumbers)
	{
		numSimpleVars = list_length(projInfo->pi_targetlist);
	}
#elif PG_VERSION_NUM < 120000
	// in GP6 we can get this value from projInfo
	numSimpleVars = projInfo->pi_numSimpleVars;
#else
	// in GP7 there is no precomputation, the numSimpleVars stays at 0
#endif
	return numSimpleVars;
};

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
static void
add_projection_desc_httpheaders(CHURL_HEADERS headers,
							   ProjectionInfo *projInfo,
							   List *qualsAttributes,
							   Relation rel)
{
	int			   i;
	int			   droppedCount;  // count of dropped attributes
	int			   headerCount;   // count of created http headers
	char		   long_number[sizeof(int32) * 8];
	int			   numSimpleVars; // number of pre-computed simple vars
	int			   *varNumbers;   // pre-computed array of simple var attrnos

	Bitmapset	   *attrs_used;   // hashset to keep and de-dup collected attrnos
	StringInfoData	formatter;
	List		   *targetList;   // targetList from the query plan
	TupleDesc	   tupdesc;

	// STEP 0: initialize variables
	initStringInfo(&formatter);
	attrs_used = NULL;
	targetList = getTargetList(projInfo);
	varNumbers = getVarNumbers(projInfo);

	// STEP 1: collect attribute numbers (attrno) from the targetList, if necessary
	if (needToIterateTargetList(targetList, varNumbers)) {
		/*
		 * we use expression_tree_walker to access attrno information
		 * we do it through a helper function add_attnums_from_targetList
		 */
		List     *l = lappend_int(NIL, 0);
		ListCell *lc1;

		foreach(lc1, targetList)
		{
			Node *node = getTargetListEntryExpression(lc1);
			add_attnums_from_targetList(node, l);
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

	// STEP 2: collect attribute numbers from pre-computed list of varNumbers (if available) of simpleVars
	numSimpleVars = getNumSimpleVars(projInfo);
	for (i = 0; varNumbers && i < numSimpleVars; i++)
	{
		attrs_used =
			bms_add_member(attrs_used,
						 varNumbers[i] - FirstLowInvalidHeapAttributeNumber);
	}

	// STEP 3: collect attribute numbers from qualifiers (WHERE conditions)
	ListCell *attribute = NULL;
	foreach(attribute, qualsAttributes)
	{
		AttrNumber attrNumber = (AttrNumber) lfirst_int(attribute);
		attrs_used =
			bms_add_member(attrs_used,
						 attrNumber + 1 - FirstLowInvalidHeapAttributeNumber);
	}

	// STEP 4: for attributes in the relation that are not dropped, add projection headers for those selected in steps 0 - 3 above
	tupdesc = RelationGetDescr(rel);
	droppedCount = 0;
	headerCount = 0;

	for (i = 1; i <= tupdesc->natts; i++)
	{
		/* Ignore dropped attributes. */
		if (TupleDescAttr(tupdesc, i - 1)->attisdropped)
		{
			/* keep a counter of the number of dropped attributes */
			droppedCount++;
			continue;
		}

		if (bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used))
		{
			/* Shift the column index by the running dropped_count */
			add_projection_index_header(headers, formatter,
										i - 1 - droppedCount, long_number);
			headerCount++;
		}
	}

	if (headerCount != 0)
	{
		/* Convert the number of projection columns to a string */
		pg_ltoa(headerCount, long_number);
		churl_headers_append(headers, "X-GP-ATTRS-PROJ", long_number);
	}

	list_free(qualsAttributes);
	pfree(formatter.data);
	bms_free(attrs_used);
}

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
static DefElem *
makeDefElem(char *name, Node *arg)
{
    DefElem    *res = makeNode(DefElem);

    res->defnamespace = NULL;
    res->defname = name;
    res->arg = arg;
    res->defaction = DEFELEM_UNSPEC;

    return res;
}

static void
parseCustomFormatString(char *fmtstr, char **formatter_name, List **formatter_params)
{
    char	   *token;
    const char *whitespace = " \t\n\r";
    char		nonstd_backslash = 0;
    int			encoding = GetDatabaseEncoding();
    List	   *l = NIL;
    bool		formatter_found = false;

    token = strtokx2(fmtstr, whitespace, NULL, NULL,
                     0, false, true, encoding);

    /* parse user custom options. take it as is. no validation needed */

    if (token)
    {
        char	   *key = token;
        char	   *val = NULL;
        StringInfoData key_modified;

        initStringInfo(&key_modified);

        while (key)
        {
            /* MPP-14467 - replace meta chars back to original */
            resetStringInfo(&key_modified);
            appendStringInfoString(&key_modified, key);
            replaceStringInfoString(&key_modified, "<gpx20>", " ");

            val = strtokx2(NULL, whitespace, NULL, "'",
                           nonstd_backslash, true, true, encoding);
            if (val)
            {

                if (pg_strcasecmp(key, "formatter") == 0)
                {
                    *formatter_name = pstrdup(val);
                    formatter_found = true;
                }
                else
                    l = lappend(l, makeDefElem(pstrdup(key_modified.data),
                                               (Node *) makeString(pstrdup(val))));
            }
            else
                goto error;

            key = strtokx2(NULL, whitespace, NULL, NULL,
                           0, false, false, encoding);
        }

    }

    if (!formatter_found)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("external table internal parse error: no formatter function name found")));

    *formatter_params = l;

    return;

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

static char *
strtokx2(const char *s,
         const char *whitespace,
         const char *delim,
         const char *quote,
         char escape,
         bool e_strings,
         bool del_quotes,
         int encoding)
{
    static char *storage = NULL;/* store the local copy of the users string
								 * here */
    static char *string = NULL; /* pointer into storage where to continue on
								 * next call */

    /* variously abused variables: */
    unsigned int offset;
    char	   *start;
    char	   *p;

    if (s)
    {
        /*
         * We may need extra space to insert delimiter nulls for adjacent
         * tokens.  2X the space is a gross overestimate, but it's unlikely
         * that this code will be used on huge strings anyway.
         */
        storage = palloc(2 * strlen(s) + 1);
        strcpy(storage, s);
        string = storage;
    }

    if (!storage)
        return NULL;

    /* skip leading whitespace */
    offset = strspn(string, whitespace);
    start = &string[offset];

    /* end of string reached? */
    if (*start == '\0')
    {
        /* technically we don't need to free here, but we're nice */
        pfree(storage);
        storage = NULL;
        string = NULL;
        return NULL;
    }

    /* test if delimiter character */
    if (delim && strchr(delim, *start))
    {
        /*
         * If not at end of string, we need to insert a null to terminate the
         * returned token.  We can just overwrite the next character if it
         * happens to be in the whitespace set ... otherwise move over the
         * rest of the string to make room.  (This is why we allocated extra
         * space above).
         */
        p = start + 1;
        if (*p != '\0')
        {
            if (!strchr(whitespace, *p))
                memmove(p + 1, p, strlen(p) + 1);
            *p = '\0';
            string = p + 1;
        }
        else
        {
            /* at end of string, so no extra work */
            string = p;
        }

        return start;
    }

    /* check for E string */
    p = start;
    if (e_strings &&
        (*p == 'E' || *p == 'e') &&
        p[1] == '\'')
    {
        quote = "'";
        escape = '\\';			/* if std strings before, not any more */
        p++;
    }

    /* test if quoting character */
    if (quote && strchr(quote, *p))
    {
        /* okay, we have a quoted token, now scan for the closer */
        char		thisquote = *p++;

        /*
         * MPP-6698 START
         *
         * unfortunately, it is possible for an external table format string
         * to be represented in the catalog in a way which is problematic to
         * parse: when using a single quote as a QUOTE or ESCAPE character the
         * format string will show [quote ''']. since we do not want to change
         * how this is stored at this point (as it will affect previous
         * versions of the software already in production) the following code
         * block will detect this scenario where 3 quote characters follow
         * each other, with no fourth one. in that case, we will skip the
         * second one (the first is skipped just above) and the last trailing
         * quote will be skipped below. the result will be the actual token
         * (''') and after stripping it due to del_quotes we'll end up with
         * ('). very ugly, but will do the job...
         */
        char		qt = quote[0];

        if (strlen(p) >= 3 && p[0] == qt && p[1] == qt && p[2] != qt)
            p++;
        /* MPP-6698 END */

        for (; *p; p += pg_encoding_mblen(encoding, p))
        {
            if (*p == escape && p[1] != '\0')
                p++;			/* process escaped anything */
            else if (*p == thisquote && p[1] == thisquote)
                p++;			/* process doubled quote */
            else if (*p == thisquote)
            {
                p++;			/* skip trailing quote */
                break;
            }
        }

        /*
         * If not at end of string, we need to insert a null to terminate the
         * returned token.  See notes above.
         */
        if (*p != '\0')
        {
            if (!strchr(whitespace, *p))
                memmove(p + 1, p, strlen(p) + 1);
            *p = '\0';
            string = p + 1;
        }
        else
        {
            /* at end of string, so no extra work */
            string = p;
        }
//
//        /* Clean up the token if caller wants that */
//        if (del_quotes)
//            strip_quotes(start, thisquote, escape, encoding);

        return start;
    }

    /*
     * Otherwise no quoting character.  Scan till next whitespace, delimiter
     * or quote.  NB: at this point, *start is known not to be '\0',
     * whitespace, delim, or quote, so we will consume at least one character.
     */
    offset = strcspn(start, whitespace);

    if (delim)
    {
        unsigned int offset2 = strcspn(start, delim);

        if (offset > offset2)
            offset = offset2;
    }

    if (quote)
    {
        unsigned int offset2 = strcspn(start, quote);

        if (offset > offset2)
            offset = offset2;
    }

    p = start + offset;

    /*
     * If not at end of string, we need to insert a null to terminate the
     * returned token.  See notes above.
     */
    if (*p != '\0')
    {
        if (!strchr(whitespace, *p))
            memmove(p + 1, p, strlen(p) + 1);
        *p = '\0';
        string = p + 1;
    }
    else
    {
        /* at end of string, so no extra work */
        string = p;
    }

    return start;
}

/*
 * Converts a character code for the format name and the formatter name into a string
 * that represents PXF transport format (TEXT or GPDBWritable)
 */
static char *
get_format_name(ExtTableEntry *exttbl)
{
	char	   *formatName = NULL;

	if (fmttype_is_text(exttbl->fmtcode) || fmttype_is_csv(exttbl->fmtcode))
	{
		formatName = TextFormatName;
	}
	else if (fmttype_is_custom(exttbl->fmtcode))
	{
		// need to determine if the formatter is pxfwritable_import or pxfwritable_export which requires
		// us to send to PXF the value of on-the-wire format as "GPDBWritable"
		if (isFormatterPxfWritable(exttbl)) {
			formatName = GpdbWritableFormatName;
		}
		else
		{
			// treat other custom formatters (such as 'fixedwidth_in') as TEXT for PXF transport
			formatName = TextFormatName;
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unable to get format name for format code: %c",
						exttbl->fmtcode)));
	}
	}

	return formatName;
}

/*
 * Checks if the custom formatter specified for the table starts with pxfwritable_ prefix
 */
static bool
isFormatterPxfWritable(ExtTableEntry *exttbl)
{
	char *formatterNameSearchString = NULL;

#if PG_VERSION_NUM >= 120000
	// for GP7 the custom formatter name is among all other options of the corresponding foreign table
	ListCell   *option;
	foreach (option, exttbl->options) {
		DefElem *defel = (DefElem *) lfirst(option);
		if (strcmp(defel->defname, "formatter") == 0) {
			formatterNameSearchString = defGetString(defel);
			break;
		}
	}
#else
	// we only have a serialized string of formatter options, parsing it requires porting a lot of code from GP6
	// instead, we will only search for occurrence of the "pxfwritable_" prefix in the whole string
	formatterNameSearchString = exttbl->fmtopts;
#endif

	if (!formatterNameSearchString || !strlen(formatterNameSearchString))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cannot determine the name of a custom formatter")));
	}

	return strstr(formatterNameSearchString, PXFWritableFormatterPrefix) != NULL;
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

/*
 * This function is copied from fileam.c::appendCopyEncodingOption() in the 6X_STABLE branch.
 * appendCopyEncodingOption() does not exist on GP5
 * and in GP7 it is now a part of gpcontrib/gp_exttable_fdw/extaccess.c that is not easily linkable,
 * so we just define it here with a different name for all versions to use
 */
static List *
appendCopyEncodingOptionToList(List *copyFmtOpts, int encoding) {
	return lappend(copyFmtOpts, makeDefElem("encoding", (Node *) makeString((char *) pg_encoding_to_char(encoding))
#if PG_VERSION_NUM >= 120000 // GP7 requires an extra parameter for makeDefElem()
					, -1
#endif
					));
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

#endif
