-- @description query01 for PXF downgrade test on small data
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

SELECT extversion FROM pg_extension WHERE extname = 'pxf';

SHOW dynamic_library_path;

SELECT p.proname, p.prosrc, p.probin
FROM pg_catalog.pg_extension AS e
    INNER JOIN pg_catalog.pg_depend AS d ON (d.refobjid = e.oid)
    INNER JOIN pg_catalog.pg_proc AS p ON (p.oid = d.objid)
WHERE d.deptype = 'e' AND e.extname = 'pxf'
ORDER BY 1;

SELECT * FROM pxf_upgrade_test ORDER BY num;

SELECT * FROM pxf_upgrade_test_multibyte ORDER BY num;
