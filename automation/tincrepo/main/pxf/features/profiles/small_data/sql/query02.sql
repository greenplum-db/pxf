-- @description query02 for PXF test on small data

SELECT name, num FROM pxf_profiles_small_data WHERE num > 50 ORDER BY name;
