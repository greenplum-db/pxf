-- @description query01 for PXF upgrade test on small data
-- start_matchsubs
--
-- m{.*/usr/local/pxf-(dev|gp\d).*}
-- s{/usr/local/pxf-(dev|gp\d)}{\$PXF_HOME}
--
-- end_matchsubs
-- start_ignore
\c pxfautomation_extension
-- end_ignore

SHOW dynamic_library_path;

SELECT p.proname, p.prosrc, p.probin
FROM pg_catalog.pg_extension AS e
    INNER JOIN pg_catalog.pg_depend AS d ON (d.refobjid = e.oid)
    INNER JOIN pg_catalog.pg_proc AS p ON (p.oid = d.objid)
WHERE d.deptype = 'e' AND e.extname = 'pxf'
ORDER BY 1;