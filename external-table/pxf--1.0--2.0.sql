/* external_table/pxf--1.0--2.0.sql */

-- No new functions were added in 2.0, but we need to recreate the existing
-- functions.
-- We expect pxf.control's MODULE_PATHNAME to point to a new location
-- outside of $GPHOME and inside $PXF_HOME.
-- We will require users to perform an `ALTER EXTENSION pxf UPDATE`
-- to reflect the new module_pathname in pg_proc.
-- To check the updated pg_proc run the following query:
-- select prosrc, probin from pg_proc where proname like '%pxf%';

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
