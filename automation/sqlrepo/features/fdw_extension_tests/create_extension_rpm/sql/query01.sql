-- @description query01 for PXF FDW extension install test
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
