CREATE TABLE gpdb_types (
    t1    text,
    t2    text,
    num1  int,
    dub1  double precision,
    dec1  numeric,
    tm timestamp,
    r real,
    bg bigint,
    b boolean,
    tn smallint,
    sml smallint,
    dt date,
    vc1 varchar(5),
    c1 char(3),
    bin bytea,
    u uuid
);

--- Replace <user> with your user
COPY gpdb_types FROM '/Users/<user>/workspace/pxf/automation/src/test/resources/data/gpdb/gpdb_types.txt';

--- Make the modifications to `gpdb_types` table then run the following file to regenerate the data:
COPY gpdb_types TO '/Users/<user>/workspace/pxf/automation/src/test/resources/data/gpdb/test_types.txt';
