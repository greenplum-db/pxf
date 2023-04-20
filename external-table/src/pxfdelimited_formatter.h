// Portions Copyright (c) 2023 VMware, Inc. or its affiliates.

#ifndef PXFDELIMITED_FORMATTER_H
#define PXFDELIMITED_FORMATTER_H

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"

#include "access/formatter.h"
#include "catalog/pg_proc.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "utils/syscache.h"
#include "utils/datetime.h"

#include "lib/stringinfo.h"

typedef struct {
	TupleDesc desc;
	Datum *values;
	bool *nulls;
	FmgrInfo *conv_functions;
	Oid *typioparams;
	char *delimiter;
	char *eol;
	char *quote;
	char *escape;
	char *quote_delimiter; /* only for searching for border, not in the config file */
	char *quote_eol; /* only for searching for border, not in the config file */
	int nColumns;

	int external_encoding;				/* remote side's character encoding */
	FmgrInfo *enc_conversion_proc;		/* conv proc from exttbl encoding to
										server or the other way around */
	bool saw_delim;
	bool saw_eol;
} pxfdelimited_state;

extern void
unpack_delimited(char *data, int len, pxfdelimited_state *myData);

#endif
