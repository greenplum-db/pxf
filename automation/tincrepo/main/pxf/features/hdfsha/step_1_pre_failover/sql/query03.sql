-- @description query03 for PXF HDFSHA test on small data in IPA-based cluster
-- similar to query01 to give PXF a chance to use a cached Kerberos token, if any

SELECT * FROM pxf_hdfsha_hdfs_ipa_no_impersonation_no_svcuser ORDER BY name;