-- @description query01 for PXF HDFSHA test on small data in IPA-based cluster

SELECT * FROM pxf_hdfsha_hdfs_ipa_no_impersonation_no_svcuser ORDER BY name;