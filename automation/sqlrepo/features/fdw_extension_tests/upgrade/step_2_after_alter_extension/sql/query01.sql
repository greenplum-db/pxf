-- @description query01 for PXF FDW extension upgrade test
-- start_matchsubs
--
-- m{.*/usr/local/pxf-(dev|gp\d).*}
-- s{/usr/local/pxf-(dev|gp\d)}{\$PXF_HOME}
--
-- m{.*\$libdir/pxf.*}
-- s{\$libdir}{\$PXF_HOME/gpextable}
--
-- end_matchsubs
-- start_ignore
\c pxfautomation_extension
-- end_ignore

SELECT extversion FROM pg_extension WHERE extname = 'pxf_fdw';

\dx+ pxf_fdw

SELECT fdw.fdwname FROM pg_catalog.pg_foreign_data_wrapper fdw where fdw.fdwname='adl_pxf_fdw';

DROP FOREIGN DATA WRAPPER abfss_pxf_fdw;
