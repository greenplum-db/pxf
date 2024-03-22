-- @description query01 for PXF FDW extension downgrade test
-- start_matchsubs
--
-- m{.*/usr/local/pxf-(dev|gp\d).*}
-- s{/usr/local/pxf-(dev|gp\d)}{\$PXF_HOME}
--
-- m{.*\$libdir/pxf.*}
-- s{\$libdir}{\$PXF_HOME/gpextable}
--
-- m{.*\"pxfdelimited_import\".*}
-- s{\"pxfdelimited_import\"}{pxfdelimited_import}
--
-- m{.*found\.*}
-- s{found\.}{found}
--
-- end_matchsubs
-- start_ignore
\c pxfautomation_extension
-- end_ignore

SELECT extversion FROM pg_extension WHERE extname = 'pxf_fdw';

\dx+ pxf_fdw

SELECT fdw.fdwname FROM pg_catalog.pg_foreign_data_wrapper fdw where fdw.fdwname='abfss_pxf_fdw';

DROP FOREIGN DATA WRAPPER adl_pxf_fdw;
