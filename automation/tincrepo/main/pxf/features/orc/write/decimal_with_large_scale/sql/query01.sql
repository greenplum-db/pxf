-- @description query01 for writing ORC decimals with scale overflow
INSERT INTO orc_decimals_with_large_scale_writable VALUES (0,false,'\x0001'::bytea,123456789000000000,10,100,'row-00',0.0,3.141592653589793,'0','var00','var-no-length-00','2010-01-01','10:11:00','2013-07-13 21:00:05.000456','1234567890123456789012345678901234567890.12345','476f35e4-da1a-43cf-8f7c-950a00000000');
