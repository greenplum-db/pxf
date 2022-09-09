#include "pxfutils.h"

#if PG_VERSION_NUM >= 90400
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#endif
#include "catalog/pg_namespace.h"
#include "utils/formatting.h"
#include "utils/syscache.h"

/*
 * Full name of the HEADER KEY expected by the PXF service
 * Converts input string to upper case and prepends "X-GP-OPTIONS-" string
 * This will be used for all user defined parameters to be isolate from internal parameters
 */
char *
normalize_key_name(const char *key)
{
	if (!key || strlen(key) == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("internal error in pxfutils.c:normalize_key_name. Parameter key is null or empty.")));
	}
// For GP 5.x latest ( 5.29.8) the server_version_num is 80323.
// For 6.x or later str_toupper doesn't work as it has different signature ( one extra parameter) so the asc_toupper will work for 6.x or 7.x
#if PG_VERSION_NUM <= 80323
    return psprintf("X-GP-OPTIONS-%s", str_toupper(pstrdup(key), strlen(key)));
#else
	return psprintf("X-GP-OPTIONS-%s", asc_toupper(pstrdup(key), strlen(key)));
#endif
}

/*
 * TypeOidGetTypename
 * Get the name of the type, given the OID
 */
char *
TypeOidGetTypename(Oid typid)
{

	Assert(OidIsValid(typid));

	HeapTuple	typtup = SearchSysCache(TYPEOID,
							ObjectIdGetDatum(typid),
							0, 0, 0);
	if (!HeapTupleIsValid(typtup))
		elog(ERROR, "cache lookup failed for type %u", typid);

	Form_pg_type typform = (Form_pg_type) GETSTRUCT(typtup);
	char	   *typname = psprintf("%s", NameStr(typform->typname));
	ReleaseSysCache(typtup);

	return typname;
}

/* Concatenate multiple literal strings using stringinfo */
char *
concat(int num_args,...)
{
	va_list		ap;
	StringInfoData str;

	initStringInfo(&str);

	va_start(ap, num_args);

	for (int i = 0; i < num_args; i++)
	{
		appendStringInfoString(&str, va_arg(ap, char *));
	}
	va_end(ap);

	return str.data;
}

/* Get authority (host:port) for the PXF server URL */
char *
get_authority(void)
{
	return psprintf("%s:%d", get_pxf_host(), get_pxf_port());
}

/* Returns the PXF Host defined in the PXF_HOST
 * environment variable or the default when undefined
 */
const char *
get_pxf_host(void)
{
	const char *hStr = getenv(ENV_PXF_HOST);
	if (hStr)
		elog(DEBUG3, "read environment variable %s=%s", ENV_PXF_HOST, hStr);
	else
		elog(DEBUG3, "environment variable %s was not supplied", ENV_PXF_HOST);
	return hStr ? hStr : PXF_DEFAULT_HOST;
}

/* Returns the PXF Port defined in the PXF_PORT
 * environment variable or the default when undefined
 */
const int
get_pxf_port(void)
{
	char *endptr = NULL;
	char *pStr = getenv(ENV_PXF_PORT);
	int port = PXF_DEFAULT_PORT;

	if (pStr) {
		port = (int) strtol(pStr, &endptr, 10);

		if (pStr == endptr)
			elog(ERROR, "unable to parse PXF port number %s=%s", ENV_PXF_PORT, pStr);
		else
			elog(DEBUG3, "read environment variable %s=%s", ENV_PXF_PORT, pStr);
	}
	else
	{
		elog(DEBUG3, "environment variable %s was not supplied", ENV_PXF_PORT);
	}

	return port;
}

/* Returns the namespace (schema) name for a given namespace oid */
char *
GetNamespaceName(Oid nsp_oid)
{
	HeapTuple	tuple;
	Datum		nspnameDatum;
	bool		isNull;

	tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nsp_oid));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
						errmsg("schema with OID %u does not exist", nsp_oid)));

	nspnameDatum = SysCacheGetAttr(NAMESPACEOID, tuple, Anum_pg_namespace_nspname,
								   &isNull);

	ReleaseSysCache(tuple);

	return DatumGetCString(nspnameDatum);
}
