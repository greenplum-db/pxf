// Portions Copyright (c) 2023 VMware, Inc. or its affiliates.

#include "multibyte_delim.h"
#include "stdio.h"

Datum multibyte_delim_import(PG_FUNCTION_ARGS);

static int
count_of_escape(char* p, char* left_border, char escape)
{
	int count = 0;
	while(p >= left_border && *p == escape)
	{
		++count;
		--p;
	}
	return count;
}

/**
 * Find the first occurrence of the target given a pointer to the beginning of the string and a pointer to the end of the string
 * @param target the character to find
 * @param left_border where to start searching
 * @param right_border where to stop searching
 * @param myData struct containing formatter option information
 * @return pointer to the character immediately after the first instance of the target
 */
static char*
find_first_ins_for_multiline(char* target, char* left_border, char* right_border, format_delimiter_state *myData)
{
	char* t = target;
	char* ret = NULL;

	if (myData->situation == WITH_QUOTE)
	{
		if (strcmp(target, myData->delimiter) == 0)
		{
			t = myData->quote_delimiter;
		}
		else
		{
			t = myData->quote_eol;
		}
	}

	char* start_pos = left_border;
	while(1)
	{
		char* p = strstr(start_pos, t);
		if (p == NULL || p > right_border - strlen(t))
		{
			break;
		}

		int escape_count = 0;

		if(myData->escape != NULL)
		{
			escape_count = count_of_escape(p-1, left_border, *(myData->escape));
		}

		if(escape_count % 2 == 0)
		{
			ret = p;
			break;
		}
		else
		{
			start_pos = p + strlen(t);
			continue;
		}
	}

	//we found a 'quote+eol', 'ret' is pointing to quote. we should return the data border that just after the quote
	if (myData->situation == WITH_QUOTE && strcmp(target, myData->eol) == 0 && ret)
	{
		++ret;
	}

	return ret;
}

/**
 * Set the values in the format_delimiter_state struct with all our formatter options
 *
 * This function assumes that the values for delimiter, quote and escape are stored in the
 * server encoding. It converts the values to the table encoding and writes it into the
 * format_delimiter_state struct
 *
 * @param fcinfo
 * @param fmt_state
 */
static void
get_config(FunctionCallInfo fcinfo, format_delimiter_state* fmt_state)
{
	fmt_state->delimiter = NULL;
	fmt_state->eol = NULL;
	fmt_state->quote = NULL;
	fmt_state->escape = NULL;

	int nargs = FORMATTER_GET_NUM_ARGS(fcinfo);

	for (int i = 1; i <= nargs; i++)
	{
		char* key = FORMATTER_GET_NTH_ARG_KEY(fcinfo, i);
		char* value = FORMATTER_GET_NTH_ARG_VAL(fcinfo, i);

		if (strcmp(key, "delimiter") == 0)
		{
			fmt_state->delimiter = value;
		}
		else if (strcmp(key, "newline") == 0)
		{
			if (pg_strcasecmp(value, "lf") == 0)
			{
				fmt_state->eol = "\n";
			}
			else if (pg_strcasecmp(value, "cr") == 0)
			{
				fmt_state->eol = "\r";
			}
			else if (pg_strcasecmp(value, "crlf") == 0)
			{
				fmt_state->eol = "\r\n";
			}
			else
			{
				// GPDB COPY command allows for a NEWLINE option with the following 3 values: LF, CRLF or CR.
				// When set, these values get interpolated correctly
				// Emulate this behavior by only allowing these three values for the multibyte delimiter formatter
				// Warning: this requires that the entire file has lines terminated in the same way.
				// (LF is used throughout the entire file)
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("NEWLINE can only be LF, CRLF, or CR")));
			}

		}
		else if (strcmp(key, "quote") == 0)
		{
			fmt_state->quote = value;
		}
		else if (strcmp(key, "escape") == 0)
		{
			fmt_state->escape = value;
		}
	}

	if (fmt_state->eol == NULL)
	{
		// while GPDB COPY framework has the ability to read the first row of data to dynamically determine
		// the newline type, we cannot do that here. Instead, assume a default value of LF
		fmt_state->eol = "\n";
	}

	//with quote, we must also have escape set it to the default if it is not provided. This is similar behavior to COPY
	if (fmt_state->quote != NULL)
	{
		if (strlen(fmt_state->quote) != 1)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("quote option must be a single character")));
		}

		if (fmt_state->escape == NULL)
		{
			// postgres defaults the escape value to be the same as quote if it is not set: https://www.postgresql.org/docs/9.4/sql-copy.html
			fmt_state->escape = fmt_state->quote;
		}
	}

	if (fmt_state->delimiter == NULL || (fmt_state->delimiter)[0] == '\0')
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("missing delimiter option")));
	}

	if(fmt_state->escape != NULL && strlen(fmt_state->escape) != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("escape option must be a single character")));
	}

	fmt_state->situation = (fmt_state->quote != NULL ? WITH_QUOTE : WITHOUT_QUOTE);

	if(fmt_state->situation == WITH_QUOTE) {
		fmt_state->quote_delimiter = palloc(strlen(fmt_state->delimiter) + 2);
		sprintf(fmt_state->quote_delimiter, "%c%s", *(fmt_state->quote), fmt_state->delimiter);

		fmt_state->quote_eol = palloc(strlen(fmt_state->eol) + 2);
		sprintf(fmt_state->quote_eol, "%c%s", *(fmt_state->quote), fmt_state->eol);
	}
}

/**
 * Initialize the format_delimiter_state struct
 * @param fcinfo
 * @return
 */
static format_delimiter_state*
new_format_delimiter_state(FunctionCallInfo fcinfo)
{
	format_delimiter_state *fmt_state;
	TupleDesc desc = FORMATTER_GET_TUPDESC(fcinfo);

	fmt_state = (format_delimiter_state*)palloc(sizeof(format_delimiter_state));
	fmt_state->desc = desc;

	int nColumns = desc->natts;
	fmt_state->values  = (Datum*)palloc(sizeof(Datum) * nColumns);
	fmt_state->nulls   = (bool*)palloc(sizeof(bool) * nColumns);

	fmt_state->conv_functions = FORMATTER_GET_CONVERSION_FUNCS(fcinfo);
	fmt_state->typioparams = FORMATTER_GET_TYPIOPARAMS(fcinfo);

	get_config(fcinfo, fmt_state);

	fmt_state->nColumns = nColumns;

	fmt_state->external_encoding = FORMATTER_GET_EXTENCODING(fcinfo);
	fmt_state->enc_conversion_proc = ((FormatterData*) fcinfo->context)->fmt_conversion_proc;

	fmt_state->saw_delim = false;
	return fmt_state;
}

/**
 * Helper function to handle any escaping that needs to be done
 * @param start pointer to the beginning of the buffer
 * @param len total length of the buffer
 * @param myData struct containing formatter options
 * @return a new buffer containing a copy of the string that has been properly escaped
 */
static char*
remove_escape(char* start, int len, format_delimiter_state *myData)
{
	char* buf = palloc(len + 1);
	int j = 0;
	int eol_len = strlen(myData->eol);
	int delimiter_len = strlen(myData->delimiter);

	for(int i = 0; i < len;)
	{
		if(start[i] == *(myData->escape))
		{
			if(myData->situation == WITH_QUOTE) // with quote, we only escape 'escape' itself and quote
			{
				if(i + 1 < len && (start[i+1] == *(myData->escape) || start[i+1] == *(myData->quote)))
				{
					buf[j++] = start[i+1];
					i = i + 2;
				}
				else //before: \a, after: \a
				{
					buf[j++] = start[i++];
				}
			}
			else // without quote, we escape delimiter, eol and 'escape' itself
			{
				if(i + 1 < len && start[i+1] == *(myData->escape))
				{
					buf[j++] = start[i+1];
					i = i + 2;
				}
				else if(i + eol_len < len && memcmp(myData->eol, start + i + 1, eol_len) == 0 )
				{
					memcpy(buf + j, myData->eol, eol_len);
					i = i + eol_len + 1;
					j += eol_len;
				}
				else if(i + delimiter_len < len && memcmp(myData->delimiter, start + i + 1, delimiter_len) == 0)
				{
					memcpy(buf + j, myData->delimiter, delimiter_len);
					i = i + delimiter_len + 1;
					j += delimiter_len;
				}
				else // we permit this, escape nothing
				{
					buf[j++] = start[i++];
				}
			}
		}
			// the former 'if' will find all 'escape + quote', so if we get into this 'if', we meet a quote not after an escape
		else if(myData->situation == WITH_QUOTE && start[i] == *(myData->quote))
		{
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
							errmsg("remove_escape: the quote needs escape"),
							errhint("Is the `escape` value in the format options set correctly?")));
		}
		else
		{
			buf[j++] = start[i++];
		}
	}

	buf[j] = 0;

	return buf;
}

/**
 * we count the quote, we need every column with two quote, return the pos of eol
 * This function ensures that the data in the buffer is indeed a complete row that can be parsed.
 * @param data the pointer to the start of the buffer
 * @param data_border the pointer to where the line/row should end
 * @param myData the struct containing formatter options
 * @return
 */
static char*
find_whole_line(char* data, char* data_border, format_delimiter_state *myData) {
	int column_cnt = myData->desc->natts;
	int delimiter_len = strlen(myData->delimiter);
	int eol_len = strlen(myData->eol);

	char* p = data;
	for(int i = 0; i < column_cnt; ++i)
	{
		//first, we check the left quote
		if(*p != *(myData->quote))
		{
			return NULL;
		}

		++p;
		while(1)
		{
			while(p < data_border && *p != *(myData->quote))
			{
				++p;
			}

			// if we didn't find the right quote in the buf
			if(p >= data_border)
			{
				return NULL;
			}

			if(myData->escape != NULL)
			{
				int cnt = count_of_escape(p-1, data, *(myData->escape));
				if(cnt % 2 == 1)
				{
					++p;
					continue;
				}
			}

			break;
		}

		// we needn't check delimiter after the last column
		if(i == column_cnt - 1)
		{
			break;
		}

		// here should be a delimiter
		++p;
		if(p > data_border - delimiter_len ||
		   (p <= data_border - delimiter_len && memcmp(p, myData->delimiter, delimiter_len) != 0) )
		{
			return NULL;
		}
		p += delimiter_len;
	}

	// we need an eol except that here is the end of buf where no need an eol
	++p;
	if(p > data_border - eol_len ||
	   (p <= data_border - eol_len && memcmp(p, myData->eol, eol_len) != 0) )
	{
		return NULL;
	}
	return p;
}

/**
 * Given a pointer to the beginning of a buffer and a length, parse the buffer into individual columns
 * @param data the pointer to the start of the buffer
 * @param len total length of the buffer
 * @param myData struct containing formatter options. it is also where the parsed data will go
 */
void
unpack_delimited(char *data, int len, format_delimiter_state *myData)
{
	char* start = (char*)data;
	char* location = (char*)data;
	char* end = (char*)data;
	StringInfo buf = makeStringInfo();
	int index = 0;
	int delimiter_len = strlen(myData->delimiter);
	int two_quote_len = (myData->situation == WITH_QUOTE ? 2 : 0); // the last quote of this column and the first quote of next column

	if(myData->situation == WITH_QUOTE)
	{
		if(*data != *(myData->quote) || data[len-1] != *(myData->quote))
		{
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
					errmsg("unpack_delimited: missing quote in row head or tail")));
		}

		//exclude the first and the last quote
		++start;
		--len;
	}

	while ( (end - (char*)data) < len)
	{
		resetStringInfo(buf);
		end = (char*)data + len;
		if (index >= myData->nColumns)
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("unpack_delimited: Column mismatch, index:%d >= nColumns:%d, more columns than expected", index, myData->nColumns)));
		}
		if (start == NULL) {
			ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					errmsg("unpack_delimited: start is a null value, index:%d >= nColumns:%d, more columns than expected", index, myData->nColumns)));
		}

		if (myData->situation == WITH_QUOTE && *(start-1) != *(myData->quote))
		{
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
					errmsg("unpack_delimited: missing quote before some column")));
		}

		location = find_first_ins_for_multiline(myData->delimiter, start, data + len, myData);

		if (location != NULL && location < end)
		{
			end = location;
			myData->saw_delim = true;
		}
		int column_len = end - start;
		if (column_len == 0)
		{
			myData->nulls[index] = true;
		}
		else
		{
			if(myData->escape == NULL)
			{
				appendBinaryStringInfo(buf, start, column_len);
			}
			else //escape the data
			{
				char* removeEscapeBuf = remove_escape(start, column_len, myData);
				appendBinaryStringInfo(buf, removeEscapeBuf, strlen(removeEscapeBuf));
				pfree(removeEscapeBuf);
			}

			// if a table encoding is provided, then we assume that the file (and thus the data stream) is in that encoding
			//  and we will need to convert the data stream from the table encoding into the server encoding
			myData->values[index] = InputFunctionCall(&myData->conv_functions[index],
#if PG_VERSION_NUM >= 90600
					buf->data, myData->typioparams[index], TupleDescAttr(myData->desc, index)->atttypmod);
#else
					buf->data, myData->typioparams[index], myData->desc->attrs[index]->atttypmod);
#endif
			myData->nulls[index] = false;
		}
		index++;

		start = location + delimiter_len + two_quote_len;
	}
	if (index < myData->nColumns)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("unpack_delimited: Column mismatch, index:%d < nColumns:%d, less columns than expected", index, myData->nColumns)));
	}
}

/**
 * Main formatter function.
 * @return
 */
Datum
multibyte_delim_import(PG_FUNCTION_ARGS)
{
	HeapTuple		   tuple;
	TupleDesc		   tupdesc;
	MemoryContext	   m, oldcontext;
	format_delimiter_state		   *myData;
	char			   *data_buf;
	int				 ncolumns = 0;
	int				 data_cur;
	int				 data_len;

	/* Must be called via the external table format manager */
	if (!CALLED_AS_FORMATTER(fcinfo))
		ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
						errmsg("cannot execute multibyte_delim_import outside format manager")));

	tupdesc = FORMATTER_GET_TUPDESC(fcinfo);

	/* Get our internal description of the formatter */
	ncolumns = tupdesc->natts;
	myData = (format_delimiter_state *) FORMATTER_GET_USER_CTX(fcinfo);

	/*
	 * Initialize the context structure
	 */
	if (myData == NULL)
	{
		myData = new_format_delimiter_state(fcinfo);
		FORMATTER_SET_USER_CTX(fcinfo, myData);
	}

	if (myData->desc->natts != ncolumns)
		elog(ERROR, "multibyte_delim_import: unexpected change of output record type");

	/* get our input data buf and number of valid bytes in it */
	data_buf = FORMATTER_GET_DATABUF(fcinfo);
	data_len = FORMATTER_GET_DATALEN(fcinfo);
	data_cur = FORMATTER_GET_DATACURSOR(fcinfo);

	int	remaining = data_len - data_cur;

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
	if (FORMATTER_GET_SAW_EOF(fcinfo))
	{
		if (remaining != 0 && !myData->saw_delim && ncolumns > 1)
		{
			if (myData->quote_delimiter != NULL)
			{
				ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("quoted delimiter (%s) not found", myData->quote_delimiter),
						errhint("Check the format options in the table definition. "
						"Additionally, make sure there are no whitespaces between the QUOTE and DELIMITER values in the data.")));
			}
		}
		else
		{
			FORMATTER_SET_BAD_ROW_DATA(fcinfo, data_buf + data_cur, remaining);
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
							errmsg("unexpected end of file (multibyte case)")));
		}

		FORMATTER_RETURN_NOTIFICATION(fcinfo, FMT_NEED_MORE_DATA);
	}

	/* start clean */
	MemSet(myData->values, 0, ncolumns * sizeof(Datum));
	MemSet(myData->nulls, true, ncolumns * sizeof(bool));
	/*
	 * tuple data extraction is done in a separate memory context
	 */
	m = FORMATTER_GET_PER_ROW_MEM_CTX(fcinfo);
	oldcontext = MemoryContextSwitchTo(m);

	FORMATTER_SET_DATACURSOR(fcinfo, data_cur);

	char* line_border = NULL;
	line_border = find_first_ins_for_multiline(myData->eol, data_buf + data_cur, data_buf + data_len, myData);
	if (line_border == NULL)
	{
		MemoryContextSwitchTo(oldcontext);
		FORMATTER_RETURN_NOTIFICATION(fcinfo, FMT_NEED_MORE_DATA);
	}

	int eol_len = strlen(myData->eol); //if we are handling the last line, perhaps there is no eol
	int delimiter_len = strlen(myData->delimiter);
	int whole_line_len = line_border - data_buf - data_cur + eol_len; //we count the eol_len;

	/*
	 *  when we found a quote+eol like `"\n`, we hope it is the last part of `" ";" ";" "\n`
	 *  the end mark of the line.
	 *  but it may be the middle part of `" ";"\n ";" "\n`
	 *  or the first part of `"\n ";" ";" "\n`.
	 *  these will confuse us, so we need count the quote to find a whole line to find the real end of the line
	 *  in the former situation, `"\n` is in the beginning
	 *  in the latter situation, there must be a delimiter like `;` before `"\n`
	 */
	if(myData->situation == WITH_QUOTE &&
	   (line_border == data_buf + data_cur + 1 || memcmp(line_border - 1  - delimiter_len, myData->delimiter, delimiter_len) == 0) )
	{
		char* real_line_border = find_whole_line(data_buf + data_cur, data_buf + data_len, myData);

		//if we can't find a whole line by counting quote, we treat this part of data as bad data
		if(real_line_border == NULL)
		{
			MemoryContextSwitchTo(oldcontext);
			FORMATTER_SET_BAD_ROW_DATA(fcinfo, data_buf + data_cur, whole_line_len);
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("there is not a whole line for this data part")));
		}
		else
		{
			line_border = real_line_border;
			whole_line_len = line_border - data_buf - data_cur + eol_len;//line_border changed
		}
	}

	PG_TRY();
	{
		// Convert input data encoding to server encoding
		char* encoded = data_buf + data_cur;
		int len = whole_line_len - eol_len;

		if (myData->external_encoding != GetDatabaseEncoding())
		{
			encoded = pg_custom_to_server(data_buf + data_cur,
										  whole_line_len - eol_len,
										  myData->external_encoding,
										  myData->enc_conversion_proc);
			len = strlen(encoded);
			// Get a complete message, unpack to myData->values and myData->nulls
			unpack_delimited(encoded, len, myData);

			// Make sure the conversion actually happened.
			if (encoded != data_buf + data_cur)
			{
				// Memory needs to be released after encoding conversion.
				pfree(encoded);
			}
		}
		else
		{
			// Get a complete message, unpack to myData->values and myData->nulls
			unpack_delimited(data_buf + data_cur, whole_line_len - eol_len, myData);
		}
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);

		FORMATTER_SET_BAD_ROW_DATA(fcinfo, data_buf + data_cur, whole_line_len);

		PG_RE_THROW();
	}
	PG_END_TRY();
	//data buffer contains a complete message, set the formatter databuf cursor
	FORMATTER_SET_DATACURSOR(fcinfo, data_cur + whole_line_len);
	/* ======================================================================= */
	MemoryContextSwitchTo(oldcontext);
	tuple = heap_form_tuple(tupdesc, myData->values, myData->nulls);
	FORMATTER_SET_TUPLE(fcinfo, tuple);
	FORMATTER_RETURN_TUPLE(tuple);
}
