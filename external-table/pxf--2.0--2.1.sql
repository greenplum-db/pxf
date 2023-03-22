------------------------------------------------------------------
-- PXF Protocol/Formatters
------------------------------------------------------------------

CREATE OR REPLACE FUNCTION pg_catalog.pxfdelimited_import() RETURNS record
AS 'MODULE_PATHNAME', 'multibyte_delim_import'
LANGUAGE C STABLE;
