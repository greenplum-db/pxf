DROP TABLE IF EXISTS ccltest;
CREATE TABLE ccltest (a int);
INSERT INTO ccltest VALUES (0);
INSERT INTO ccltest VALUES (1);
SELECT a FROM ccltest ORDER BY a;
DROP TABLE ccltest;

