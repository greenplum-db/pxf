// Portions Copyright (c) 2023 VMware, Inc. or its affiliates.

#ifndef MULTIBYTE_DELIM_H
#define MULTIBYTE_DELIM_H

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

/* Do the module magic dance */
//PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(multibyte_delim_import);

typedef enum DelimitedSituation
{
    WITHOUT_QUOTE,
    WITH_QUOTE
} DelimitedSituation;

typedef struct {
    TupleDesc desc;
    Datum               *values;
    bool                *nulls;
    FmgrInfo            *conv_functions;
    Oid                 *typioparams;
    char*               delimiter;
    char*               eol_prefix;
    char*               eol;
    char*               quote;
    char*               escape;
    char*               quote_delimiter;//these two only for searching for border, not in the config file
    char*               quote_eol;
    uint32_t            length;
    /*
      when handling multiple line with one header, one line one call,
      so we need remember the pos of next line in header's length next time.
    */
    uint32_t            current_pos;
    int                 nColumns;
    DelimitedSituation  situation;
} format_delimiter_state;

extern void
unpack_delimited(char *data, int len, format_delimiter_state *myData);
#endif
