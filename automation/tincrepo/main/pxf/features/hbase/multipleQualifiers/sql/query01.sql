-- @description query01 for PXF HBase - multiple qualifiers filter

SELECT * from pxf_hbase_table WHERE recordkey != '00000002' AND "cf1:q3" > 6  AND "cf1:q8" < 10 AND "cf1:q9" > 0 ORDER BY recordkey;
