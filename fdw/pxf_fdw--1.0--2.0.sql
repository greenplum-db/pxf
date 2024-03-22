/* fdw/pxf_fdw--1.0--2.0.sql */

-- remove the foreign data wrapper from the extension
ALTER EXTENSION pxf_fdw DROP FOREIGN DATA WRAPPER adl_pxf_fdw;

-- remove the foreign data wrapper itself from the catalog
DROP FOREIGN DATA WRAPPER adl_pxf_fdw;

CREATE FOREIGN DATA WRAPPER abfss_pxf_fdw
    HANDLER pxf_fdw_handler
    VALIDATOR pxf_fdw_validator
    OPTIONS ( protocol 'abfss', mpp_execute 'all segments' );
