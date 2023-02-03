/*-------------------------------------------------------------------------
 *
 * gpdbwritableformatter.c
 *
 * This is the GPDB side for serializing and deserializing a tuple to a
 * common format which can be parsed/generated by Hadoop's GPDBWritable.
 *
 * The serialized form produced by gpdbwritableformatter_export is identical to
 * GPDBWritable.write(DataOutput out).
 *
 * The deserialization gpwritableformatter_import can deserialize the
 * bytes produced by gpdbwritableformatter_export and GPDBWritable.write.
 *
 *
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
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "access/formatter.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

#include <unistd.h>
#if PG_VERSION_NUM >= 120000
#include "access/external.h"
#endif

PG_FUNCTION_INFO_V1(gpdbwritableformatter_export);
PG_FUNCTION_INFO_V1(gpdbwritableformatter_import);
Datum		gpdbwritableformatter_import(PG_FUNCTION_ARGS);
Datum		gpdbwritableformatter_export(PG_FUNCTION_ARGS);

static const int ERR_COL_OFFSET = 9;
static const int FIRST_LINE_NUM = 1;

typedef struct
{
	/* The Datum/null of the tuple */
	Datum	   *values;
	bool	   *nulls;

	int			lineno;

	/* The export formatted value/len */
	char	  **outval;
	int		   *outlen;
	int		   *outpadlen;		/* padding length for alignment */

	/* Buffer to hold the export formatted tuple */
	StringInfo	export_format_tuple;

	/*
	 * (Binary) In/Out functions.
	 *
	 * For import, it's input function; for export, it's output function. If
	 * the type is using binary format, the function will the be binary
	 * conversion function.
	 */
	FmgrInfo   *io_functions;
	Oid		   *typioparams;
} format_t;


/*
 * Serialize the object using the following format:
 * Total Length | Version | error	| #columns | Col type | Col type |... | Null Bit array   | Col val...
 * 4 byte		| 2 byte	1 byte	| 2 byte	   2 byte     2 byte	      ceil(#col/8) byte	 fixed length or var len
 *
 * For fixed length type, we know the length.
 * In the col val, we align pad according to the alignment requirement of the type.
 * For var length type, the alignment is always 4 byte.
 * For var length type, col val is <4 byte length><payload val>
 */
#define GPDBWRITABLE_VERSION 2
/* for backward compatibility */
#define GPDBWRITABLE_PREV_VERSION 1

#define FORMATTER_ENCODING_ERR_MSG "pxfwritable_%s formatter can only %s UTF8 formatted data. Define the external table with ENCODING UTF8"

/* Bit flag */
#define GPDBWRITABLE_BITFLAG_ISNULL 1	/* Column is null */

#if PG_VERSION_NUM >= 90400
/*
 * appendStringInfoFill
 *
 * Append a single byte, repeated 0 or more times, to str.
 */
static void
appendStringInfoFill(StringInfo str, int occurrences, char ch)
{
	/* Length must not overflow. */
	if (str->len + occurrences <= str->len)
		return;

	/* Make more room if needed */
	if (str->len + occurrences >= str->maxlen)
		enlargeStringInfo(str, occurrences);

	/* Fill specified number of bytes with the character. */
	memset(str->data + str->len, ch, occurrences);
	str->len += occurrences;
	str->data[str->len] = '\0';
}
#endif

#ifndef unlikely
#if __GNUC__ > 3
#define unlikely(x) __builtin_expect((x) != 0, 0)
#else
#define unlikely(x) ((x) != 0)
#endif
#endif

#define ENSURE_BUF_LEN(buf_len, buf_idx, len_wanted) \
	do { 	\
		if (unlikely((buf_len) - (buf_idx) < (len_wanted) || (len_wanted < 0))) { \
			ereport(FATAL, (errprintstack(true), \
			                errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION), \
			                errmsg("buffer is too small: buf_len=%d, len_wanted=%d", \
			                       (buf_len) - (buf_idx), (len_wanted)))); \
		} \
	} while (0)

#if PG_VERSION_NUM < 90400
// Copied from tupdesc.h (6.x), since this is not present in GPDB 5
/* Accessor for the i'th attribute of tupdesc. */
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])
#endif

/*
 * Write a int4 to the buffer
 */
static void
appendIntToBuffer(StringInfo buf, int val)
{
	uint32		n32 = htonl((uint32) val);

	appendBinaryStringInfo(buf, (char *) &n32, 4);
}

/*
 * Read a int from the buffer, given the offset;
 * it will return the int value and increase the offset
 */
static int
readIntFromBuffer(char *buffer, int buf_len, int *offset)
{
	uint32		n32;

	ENSURE_BUF_LEN(buf_len, *offset, 4);
	memcpy(&n32, &buffer[*offset], sizeof(int));
	*offset += 4;
	return ntohl(n32);
}

/*
 * Write a int2 to the buffer
 */
static void
appendInt2ToBuffer(StringInfo buf, uint16 val)
{
	uint16		n16 = htons((uint16) val);

	appendBinaryStringInfo(buf, (char *) &n16, 2);
}

/*
 * Read a int2 from the buffer, given the offset;
 * it will return the int value and increase the offset
 */
static uint16
readInt2FromBuffer(char *buffer, int buf_len, int *offset)
{
	uint16		n16;

	ENSURE_BUF_LEN(buf_len, *offset, 2);
	memcpy(&n16, &buffer[*offset], sizeof(uint16));
	*offset += 2;
	return ntohs(n16);
}

/*
 * Write a int1 to the buffer
 */
static void
appendInt1ToBuffer(StringInfo buf, uint8 val)
{
	unsigned char n8;

	n8 = (unsigned char) val;
	appendBinaryStringInfo(buf, (char *) &n8, 1);
}

/*
 * Read a int1 from the buffer, given the offset;
 * it will return the int value and increase the offset
 */
static uint8
readInt1FromBuffer(char *buffer, int buf_len, int *offset)
{
	uint8		n8;

	ENSURE_BUF_LEN(buf_len, *offset, 1);
	memcpy(&n8, &buffer[*offset], sizeof(uint8));
	*offset += 1;
	return n8;
}

/*
 * Tells whether the given type will be formatted as binary or text
 */
static inline bool
isBinaryFormatType(Oid typeid)
{
	/* For version 1, we support binary format for these type */
	return (typeid == BOOLOID ||
			typeid == BYTEAOID ||
			typeid == FLOAT4OID ||
			typeid == FLOAT8OID ||
			typeid == INT2OID ||
			typeid == INT4OID ||
			typeid == INT8OID);
}

/*
 * Tells whether the given type is variable length
 */
static inline bool
isVariableLength(Oid typeid)
{
	return (typeid == BYTEAOID || !isBinaryFormatType(typeid));
}

/*
 * Convert typeOID to java enum DBType.ordinal()
 */
static inline int8
getJavaEnumOrdinal(Oid typeid)
{
	/* For version 1, we support binary format for these type */
	switch (typeid)
	{
		case INT8OID:
			return 0;
		case BOOLOID:
			return 1;
		case FLOAT8OID:
			return 2;
		case INT4OID:
			return 3;
		case FLOAT4OID:
			return 4;
		case INT2OID:
			return 5;
		case BYTEAOID:
			return 6;
	}
	return 7;
}

/*
 * Convert java enum DBType.ordinal() to typeOID
 */
static inline Oid
getTypeOidFromJavaEnumOrdinal(int8 enumType)
{
	switch (enumType)
	{
		case 0:
			return INT8OID;
		case 1:
			return BOOLOID;
		case 2:
			return FLOAT8OID;
		case 3:
			return INT4OID;
		case 4:
			return FLOAT4OID;
		case 5:
			return INT2OID;
		case 6:
			return BYTEAOID;
		case 7:
			return TEXTOID;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("ill-formatted record: unknown Java Enum Ordinal (%d)",
							enumType)));
	}
	return 0;
}

/*
 * Helper to determine the size of the null byte array
 */
static int
getNullByteArraySize(int colCnt)
{
	return (colCnt / 8) + (colCnt % 8 != 0 ? 1 : 0);
}

/*
 * Helper routine to convert boolean array to byte array.
 *
 * This routine is aware of the attributes in the table, and it will only
 * produce a result for valid columns (excludes dropped columns).
 */
static bits8 *
boolArrayToByteArray(bool *data, int len, int validlen, int *outlen, TupleDesc tupdesc)
{
	int			i,
				j,
				k;
	bits8	   *result;

	*outlen = getNullByteArraySize(validlen);
	result = palloc0(*outlen * sizeof(bits8));

	for (i = 0, j = 0, k = 7; i < len; i++)
	{
		/* Ignore dropped attributes. */
		Form_pg_attribute attr = TupleDescAttr(tupdesc,i);

		if (attr->attisdropped)
			continue;

		result[j] |= (data[i] ? 1 : 0) << k--;
		if (k < 0)
		{
			j++;
			k = 7;
		}
	}
	return result;
}

/*
 * Helper routine to convert byte array to boolean array
 * It'll write the output to booldata.
 *
 * This routine supports dropped columns, in the case of tables with dropped
 * columns booldata's size will match the number of original columns
 * (including dropped columns), and the source `data` will match the number
 * of columns provided by the PXF server, so if there are dropped columns,
 * PXF server will only provide the subset of columns. Below a graphical
 * representation of the mapping.
 *
 *  --------------------------------------------
 * |  col1  |  col2  |  col3  |  col5  |  col6  |  input: *data
 *  --------------------------------------------
 *     |        |        |         |        └----------------⬎
 *     ↓        ↓        ↓         └----------------⬎        ↓
 *  -------------------------------------------------------------
 * |  col1  |  col2  |  col3  |  col4 (dropped)  | col5  | col6  | output: **booldata
 *  -------------------------------------------------------------
 */
static void
byteArrayToBoolArray(bits8 *data, int data_len, int len, bool **booldata, int boollen, TupleDesc tupdesc)
{
	int			i,
				j,
				k;

	ENSURE_BUF_LEN(data_len, 0, len);
	for (i = 0, j = 0, k = 7; i < boollen; i++)
	{
		/* Ignore dropped attributes. */
		Form_pg_attribute attr = TupleDescAttr(tupdesc,i);

		if (attr->attisdropped)
		{
			(*booldata)[i] = true;
			continue;
		}

		(*booldata)[i] = ((data[j] >> k--) & 0x01) == 1;
		if (k < 0)
		{
			j++;
			k = 7;
		}
	}
}

/*
 * Verify external table definition matches to input data columns
 */
static void
verifyExternalTableDefinition(int16 ncolumns_remote, AttrNumber nvalidcolumns, AttrNumber ncolumns, TupleDesc tupdesc,
                              char *data_buf, int data_len, int *bufidx)
{
	int			   i;
	StringInfoData errMsg;
	Oid			   input_type;
	Oid			   defined_type;
	int8		   enumType;

	if (ncolumns_remote != nvalidcolumns)
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						errmsg("input data column count (%d) did not match the external table definition",
							   ncolumns_remote)));

	initStringInfo(&errMsg);

	/* Extract Column Type and check against External Table definition */
	for (i = 0; i < ncolumns; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc,i);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		input_type = 0;
		defined_type = attr->atttypid;
		enumType = readInt1FromBuffer(data_buf, data_len, bufidx);

		/* Convert enumType to type oid */
		input_type = getTypeOidFromJavaEnumOrdinal(enumType);
		if ((isBinaryFormatType(defined_type) || isBinaryFormatType(input_type)) &&
			input_type != defined_type)
		{
			char	   *intype = format_type_be(input_type);
			char	   *deftype = format_type_be(defined_type);
			char	   *attname = NameStr(attr->attname);

			if (errMsg.len > 0)
				appendStringInfoString(&errMsg, ", ");

			appendStringInfo(&errMsg, "column \"%s\" (type \"%s\", input data type \"%s\")",
							 attname, deftype, intype);
			pfree(intype);
			pfree(deftype);
		}
	}
	if (errMsg.len > 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("external table definition did not match input data: %s",
						errMsg.data)));
	}
}

Datum
gpdbwritableformatter_export(PG_FUNCTION_ARGS)
{
	HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
	TupleDesc	tupdesc;
	HeapTupleData tuple;
	format_t   *myData;
	int			datlen;
	AttrNumber	ncolumns;
	AttrNumber	nvalidcolumns;
	AttrNumber	i;
	MemoryContext per_row_ctx,
				oldcontext;
	bits8	   *nullBit;
	int			nullBitLen;
	int			endpadding;

	/* Must be called via the external table format manager */
	if (!CALLED_AS_FORMATTER(fcinfo))
		ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
						errmsg("cannot execute gpdbwritableformatter_export outside format manager")));

	tupdesc = FORMATTER_GET_TUPDESC(fcinfo);

	/* Get our internal description of the formatter */
	ncolumns = tupdesc->natts;
	myData = (format_t *) FORMATTER_GET_USER_CTX(fcinfo);

	/* Get the number of valid columns, excludes dropped columns */
	nvalidcolumns = 0;
	for (i = 0; i < ncolumns; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc,i);

		if (!attr->attisdropped)
			nvalidcolumns++;
	}

	/*
	 * Initialize the context structure
	 */
	if (myData == NULL)
	{
		if (FORMATTER_GET_EXTENCODING(fcinfo) != PG_UTF8)
		{
		     ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
		            errmsg(FORMATTER_ENCODING_ERR_MSG, "export", "export")));
		}

		myData = palloc(sizeof(format_t));
		myData->values = palloc(sizeof(Datum) * ncolumns);
		myData->outval = palloc(sizeof(char *) * ncolumns);
		myData->nulls = palloc(sizeof(bool) * ncolumns);
		myData->outlen = palloc(sizeof(int) * ncolumns);
		myData->outpadlen = palloc(sizeof(int) * ncolumns);
		myData->io_functions = palloc(sizeof(FmgrInfo) * ncolumns);
		myData->export_format_tuple = makeStringInfo();

		/* setup the text/binary input function */
		for (i = 0; i < ncolumns; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc,i);

			Oid			type = attr->atttypid;
			bool		isvarlena;
			Oid			functionId;

			/* Ignore dropped attributes. */
			if (attr->attisdropped)
				continue;

			/* Get the text/binary "send" function */
			if (isBinaryFormatType(type))
				getTypeBinaryOutputInfo(type, &(functionId), &isvarlena);
			else
				getTypeOutputInfo(type, &(functionId), &isvarlena);
			fmgr_info(functionId, &(myData->io_functions[i]));
		}

		FORMATTER_SET_USER_CTX(fcinfo, myData);
	}

	per_row_ctx = FORMATTER_GET_PER_ROW_MEM_CTX(fcinfo);
	oldcontext = MemoryContextSwitchTo(per_row_ctx);

	/* break the input tuple into fields */
	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_data = rec;
	heap_deform_tuple(&tuple, tupdesc, myData->values, myData->nulls);

	/*
	 * Starting from here. The conversion to bytes is  exactly the same as
	 * GPDBWritable.toBytes()
	 */

	/*-----
	 * Now, compute the total payload and header length (#col excludes
	 * dropped columns):
	 *
	 * header = total length (4 byte), Version (2 byte), Error (1 byte), #col (2 byte)
	 * col type array = #col * 1 byte
	 * null bit array = ceil(#col/8)
	 *-----
	 */
	datlen = sizeof(int32) + sizeof(int16) + sizeof(int8) + sizeof(int16);
	datlen += nvalidcolumns;
	datlen += getNullByteArraySize(nvalidcolumns);

	/*
	 * We need to know the total length of the tuple. So, we've to transformed
	 * each column so that we know the transformed size and the alignment
	 * padding.
	 *
	 * Since we're computing the conversion function, we use per-row memory
	 * context inside the loop.
	 */
	for (i = 0; i < ncolumns; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc,i);

		/* Ignore dropped attributes. */
		if (attr->attisdropped) continue;

		Oid			type = attr->atttypid;
		Datum		val = myData->values[i];
		bool		nul = myData->nulls[i];
		FmgrInfo   *iofunc = &(myData->io_functions[i]);
		int			alignpadlen = 0;

		if (nul)
			myData->outlen[i] = 0;
		else
		{
			if (isBinaryFormatType(type))
			{
				bytea	   *tmpval = SendFunctionCall(iofunc, val);;

				/* NOTE: exclude the header length */
				myData->outval[i] = VARDATA(tmpval);
				myData->outlen[i] = VARSIZE_ANY_EXHDR(tmpval);
			}
			else
			{
				/* NOTE: include the "\0" in the length for text format */
				myData->outval[i] = OutputFunctionCall(iofunc, val);
				myData->outlen[i] = strlen(myData->outval[i]) + 1;
			}

			/*
			 * For variable length type, we added a 4 byte length header. So,
			 * it'll be aligned int4. For fixed length type, we'll use the
			 * type alignment.
			 */
			if (isVariableLength(type))
			{
				alignpadlen = INTALIGN(datlen) - datlen;
				datlen += sizeof(int32);
			}
			else
				alignpadlen = att_align_nominal(datlen, attr->attalign) - datlen;
			myData->outpadlen[i] = alignpadlen;
			datlen += alignpadlen;
		}
		datlen += myData->outlen[i];
	}

	/*
	 * Add the final alignment padding for the next record
	 */
	endpadding = DOUBLEALIGN(datlen) - datlen;
	datlen += endpadding;

	/*
	 * Now, we're done with per-row computation. Switch back to the old memory
	 * context.
	 */
	MemoryContextSwitchTo(oldcontext);

	/*
	 * Resize buffer, if needed The new size includes the 4 byte VARHDSZ, the
	 * entire payload and 1 more byte for '\0' that StringInfo always ends
	 * with.
	 */
	if (myData->export_format_tuple->maxlen < VARHDRSZ + datlen + 1)
	{
		pfree(myData->export_format_tuple->data);
		initStringInfoOfSize(myData->export_format_tuple, VARHDRSZ + datlen + 1);
	}

	/* Reset the export format buffer */
	resetStringInfo(myData->export_format_tuple);

	/* Reserve VARHDRSZ bytes for the bytea length word */
	appendStringInfoFill(myData->export_format_tuple, VARHDRSZ, '\0');

	/* Construct the packet header */
	appendIntToBuffer(myData->export_format_tuple, datlen);
	appendInt2ToBuffer(myData->export_format_tuple, GPDBWRITABLE_VERSION);
	appendInt1ToBuffer(myData->export_format_tuple, 0); /* error */
	appendInt2ToBuffer(myData->export_format_tuple, nvalidcolumns);

	/* Write col type for columns that have not been dropped */
	for (i = 0; i < ncolumns; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc,i);

		/* Ignore dropped attributes. */
		if (!attr->attisdropped)
		{
			appendInt1ToBuffer(myData->export_format_tuple,
							   getJavaEnumOrdinal(attr->atttypid));
		}
	}

	/* Write Nullness */
	nullBit = boolArrayToByteArray(myData->nulls, ncolumns, nvalidcolumns, &nullBitLen, tupdesc);
	appendBinaryStringInfo(myData->export_format_tuple, (char *) nullBit, nullBitLen);

	/* Column Value */
	for (i = 0; i < ncolumns; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc,i);

		/* Ignore dropped attributes and null values. */
		if (!attr->attisdropped && !myData->nulls[i])
		{
			/* Pad the alignment byte first */
			appendStringInfoFill(myData->export_format_tuple, myData->outpadlen[i], '\0');

			/* For variable length type, we added a 4 byte length header */
			if (isVariableLength(attr->atttypid))
				appendIntToBuffer(myData->export_format_tuple, myData->outlen[i]);

			/* Now, write the actual column value */
			appendBinaryStringInfo(myData->export_format_tuple,
								   myData->outval[i], myData->outlen[i]);
		}
	}

	/* End padding */
	appendStringInfoFill(myData->export_format_tuple, endpadding, '\0');

	if (myData->export_format_tuple->len != datlen + VARHDRSZ)
	{
		ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					errmsg("Tuple length doesn't match the data length")));
	}

	SET_VARSIZE(myData->export_format_tuple->data, datlen + VARHDRSZ);
	PG_RETURN_BYTEA_P(myData->export_format_tuple->data);
}

Datum
gpdbwritableformatter_import(PG_FUNCTION_ARGS)
{
	HeapTuple	tuple;
	TupleDesc	tupdesc;
	MemoryContext per_row_ctx,
				oldcontext;
	format_t   *myData;
	AttrNumber	ncolumns;
	AttrNumber	nvalidcolumns = 0;
	AttrNumber	i;
	char	   *data_buf;
	int			data_cur;
	int			data_len;
	int			tuplelen;
	int			bufidx = 0;
	int			tupleEndIdx = 0;
	int16		version;
	int8		error_flag = 0;
	int16		ncolumns_remote = 0;
	int			remaining = 0;

	/* Must be called via the external table format manager */
	if (!CALLED_AS_FORMATTER(fcinfo))
		ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
						errmsg("cannot execute gpdbwritableformatter_import outside format manager")));

	tupdesc = FORMATTER_GET_TUPDESC(fcinfo);

	/* Get our internal description of the formatter */
	ncolumns = tupdesc->natts;
	myData = (format_t *) FORMATTER_GET_USER_CTX(fcinfo);

	/* Get the number of valid columns, excluding dropped columns */
	for (i = 0; i < ncolumns; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc,i);
		if (!attr->attisdropped)
			nvalidcolumns++;
	}

	/*
	 * Initialize the context structure
	 */
	if (myData == NULL)
	{
		if (FORMATTER_GET_EXTENCODING(fcinfo) != PG_UTF8)
		{
			ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
							errmsg(FORMATTER_ENCODING_ERR_MSG, "import", "import")));
		}

		myData = palloc(sizeof(format_t));
		myData->values = palloc(sizeof(Datum) * ncolumns);
		myData->nulls = palloc(sizeof(bool) * ncolumns);
		myData->lineno = FIRST_LINE_NUM;
		myData->outlen = palloc(sizeof(int) * ncolumns);
		myData->typioparams = (Oid *) palloc(ncolumns * sizeof(Oid));
		myData->io_functions = palloc(sizeof(FmgrInfo) * ncolumns);

		for (i = 0; i < ncolumns; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc,i);

			Oid type = attr->atttypid;

			Oid			functionId;

			/* Ignore dropped attributes. */
			if (attr->attisdropped)
				continue;

			/* Get the text/binary "receive" function */
			if (isBinaryFormatType(type))
				getTypeBinaryInputInfo(type, &(functionId), &myData->typioparams[i]);
			else
				getTypeInputInfo(type, &(functionId), &myData->typioparams[i]);
			fmgr_info(functionId, &(myData->io_functions[i]));
		}

		FORMATTER_SET_USER_CTX(fcinfo, myData);
	}

	/* get our input data buf and number of valid bytes in it */
	data_buf = FORMATTER_GET_DATABUF(fcinfo);
	data_len = FORMATTER_GET_DATALEN(fcinfo);
	data_cur = FORMATTER_GET_DATACURSOR(fcinfo);

	/*--------------------------------
	 * MAIN FORMATTING CODE
	 *--------------------------------
	 */

	/* Get the first 4 byte; That's the length of the entire packet */
	remaining = data_len - data_cur;
	bufidx = data_cur;

	/*
	 * NOTE: Unexpected EOF Error Handling
	 *
	 * The first time we noticed an unexpected EOF, we'll set the datacursor
	 * forward and then raise the error. But then, the framework will still
	 * call the formatter the function again. Now, the formatter function will
	 * be provided with a zero length data buffer. In this case, we should not
	 * raise an error again, but simply return "NEED MORE DATA". This is how
	 * the formatter framework works.
	 */
	if (remaining == 0 && FORMATTER_GET_SAW_EOF(fcinfo))
		FORMATTER_RETURN_NOTIFICATION(fcinfo, FMT_NEED_MORE_DATA);

	if (remaining < 4)
	{
		if (FORMATTER_GET_SAW_EOF(fcinfo))
		{
			FORMATTER_SET_BAD_ROW_DATA(fcinfo, data_buf + data_cur, remaining);
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
							errmsg("unexpected end of file")));
		}
		FORMATTER_RETURN_NOTIFICATION(fcinfo, FMT_NEED_MORE_DATA);
	}

	tuplelen = readIntFromBuffer(data_buf, data_len, &bufidx);

	/* calculate the index of last byte of this tuple in data_buf */
	tupleEndIdx = data_cur + tuplelen;

	/* Now, make sure we've received the entire tuple */
	if (remaining < tuplelen)
	{
		if (FORMATTER_GET_SAW_EOF(fcinfo))
		{
			FORMATTER_SET_BAD_ROW_DATA(fcinfo, data_buf + data_cur, remaining);
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
							errmsg("unexpected end of file")));
		}
		FORMATTER_RETURN_NOTIFICATION(fcinfo, FMT_NEED_MORE_DATA);
	}

	/* We got here. So, we've the ENTIRE tuple in the buffer */
	FORMATTER_SET_BAD_ROW_DATA(fcinfo, data_buf + data_cur, tuplelen);

	/* start clean */
	MemSet(myData->values, 0, ncolumns * sizeof(Datum));
	MemSet(myData->nulls, true, ncolumns * sizeof(bool));

	per_row_ctx = FORMATTER_GET_PER_ROW_MEM_CTX(fcinfo);
	oldcontext = MemoryContextSwitchTo(per_row_ctx);

	/* extract the version, error and column count */
	version = readInt2FromBuffer(data_buf, tupleEndIdx, &bufidx);

	if ((version != GPDBWRITABLE_VERSION) && (version != GPDBWRITABLE_PREV_VERSION))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot import data version %d", version)));

	if (version == GPDBWRITABLE_VERSION)
		error_flag = readInt1FromBuffer(data_buf, tupleEndIdx, &bufidx);

	if (error_flag) {
		bufidx += ERR_COL_OFFSET;
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("%.*s", tupleEndIdx - bufidx, data_buf + bufidx)));
	}

	ncolumns_remote = readInt2FromBuffer(data_buf, tupleEndIdx, &bufidx);

	verifyExternalTableDefinition(ncolumns_remote, nvalidcolumns, ncolumns, tupdesc, data_buf, tupleEndIdx, &bufidx);

	/* Extract null bit array */
	{
		int			nullByteLen = getNullByteArraySize(ncolumns_remote);
		bits8	   *nullByteArray = (bits8 *) (data_buf + bufidx);

		byteArrayToBoolArray(nullByteArray, tupleEndIdx - bufidx, nullByteLen, &myData->nulls, ncolumns, tupdesc);
		bufidx += nullByteLen;
	}

	/* extract column value */
	for (i = 0; i < ncolumns; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc,i);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (!myData->nulls[i])
		{
			FmgrInfo   *iofunc = &(myData->io_functions[i]);

			/*
			 * Skip the alignment padding for variable length type: always
			 * align int4 because we're reading a length header. we'll get the
			 * payload length from the first 4 byte.
			 */
			if (isVariableLength(attr->atttypid))
			{
				bufidx = INTALIGN(bufidx);
				myData->outlen[i] = readIntFromBuffer(data_buf, tupleEndIdx, &bufidx);
			}

			/*
			 * Skip the alignment padding for fixed length type: use the type
			 * alignment. we can use the type length attribute.
			 */
			else
			{
				bufidx = att_align_nominal(bufidx, attr->attalign);
				myData->outlen[i] = attr->attlen;
			}

			/* check that the length of the i-th value is not negative and fits
			 * within the remaining space for the current tuple in data_buf
			 */
			if (myData->outlen[i] < 0 || (tupleEndIdx - bufidx) < myData->outlen[i])
				ereport(FATAL,
				        (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				         errmsg("data for column %d of row %d has invalid length",
				                i + 1, myData->lineno),
				         errdetail("total length for tuple is %d bytes, remaining bytes is %d, length for column %d is %d bytes",
				                   tuplelen, tupleEndIdx - bufidx, i + 1, myData->outlen[i])));

			if (isBinaryFormatType(attr->atttypid))
			{
				StringInfoData tmpbuf;

				tmpbuf.data = data_buf + bufidx;
				tmpbuf.maxlen = myData->outlen[i];
				tmpbuf.len = myData->outlen[i];
				tmpbuf.cursor = 0;

				myData->values[i] = ReceiveFunctionCall(iofunc,
														&tmpbuf,
														myData->typioparams[i],
														attr->atttypmod);
			}
			else
			{
				/*
				 * Read string value from data_buf and convert it to internal representation
				 *
				 * PXF service should send all strings with a terminating nul-byte, so we can
				 * determine the number of bytes that the input function will read and compare
				 * it with the length that the PXF service computed and sent. Since we've
				 * checked that outlen[i] is no larger than the number of bytes left for this
				 * tuple in data_buf, we use it as max number of bytes to scan in the call to
				 * strnlen
				 */
				size_t actual_len = strnlen(data_buf + bufidx, myData->outlen[i]);

				/*
				 * Compare the length as returned by strnlen with the length as determined by
				 * the PXF service. If the source data includes ASCII NUL-byte in the string,
				 * then these two values will not match. It's possible that this will result
				 * in the Postgres input function truncating/mis-reading the value. Rather
				 * than treat this as an error here, we log a debug message.
				 */
				if (actual_len != myData->outlen[i]-1)
					ereport(DEBUG1,
							(errmsg("expected column %d of row %d to have length %d, actual length is %ld", i+1, myData->lineno, myData->outlen[i]-1, actual_len)));

				myData->values[i] = InputFunctionCall(iofunc,
													  data_buf + bufidx,
													  myData->typioparams[i],
													  attr->atttypmod);
			}
			bufidx += myData->outlen[i];
		}
	}
	bufidx = DOUBLEALIGN(bufidx);

	if (tupleEndIdx != bufidx)
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("tuplelen != bufidx: %d:%d:%d", tuplelen, bufidx, data_cur)));

	data_cur += tuplelen;

	MemoryContextSwitchTo(oldcontext);

	FORMATTER_SET_DATACURSOR(fcinfo, data_cur);
	tuple = heap_form_tuple(tupdesc, myData->values, myData->nulls);
	FORMATTER_SET_TUPLE(fcinfo, tuple);
	FORMATTER_RETURN_TUPLE(tuple);
}
