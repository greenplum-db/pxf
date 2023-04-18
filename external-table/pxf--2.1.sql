------------------------------------------------------------------
-- PXF Protocol/Formatters
------------------------------------------------------------------

CREATE OR REPLACE FUNCTION pg_catalog.pxf_write() RETURNS integer
AS 'MODULE_PATHNAME', 'pxfprotocol_export'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.pxf_read() RETURNS integer
AS 'MODULE_PATHNAME', 'pxfprotocol_import'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.pxf_validate() RETURNS void
AS 'MODULE_PATHNAME', 'pxfprotocol_validate_urls'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.pxfwritable_import() RETURNS record
AS 'MODULE_PATHNAME', 'gpdbwritableformatter_import'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.pxfwritable_export(record) RETURNS bytea
AS 'MODULE_PATHNAME', 'gpdbwritableformatter_export'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.pxfdelimited_import() RETURNS record
AS 'MODULE_PATHNAME', 'pxfdelimited_import'
LANGUAGE C STABLE;

CREATE TRUSTED PROTOCOL pxf (
  writefunc     = pxf_write,
  readfunc      = pxf_read,
  validatorfunc = pxf_validate);
