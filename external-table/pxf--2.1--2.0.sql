------------------------------------------------------------------
-- PXF Protocol/Formatters
------------------------------------------------------------------

-- remove the function from the extension
ALTER EXTENSION pxf DROP FUNCTION pg_catalog.pxfdelimited_import();

-- remove the function itself from the catalog
DROP FUNCTION pg_catalog.pxfdelimited_import();