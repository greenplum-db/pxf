DROP TABLE IF EXISTS gploadtable;
CREATE TABLE gploadtable(l_orderkey integer,l_partkey integer,l_suppkey integer,l_linenumber integer,l_quantity decimal(15,2),l_extendedprice decimal(15,2),l_discount decimal(15,2),l_tax decimal(15,2),l_returnflag char(1),l_linestatus char(1),l_shipdate date,l_commitdate date,l_receiptdate date,l_shipinstruct char(25),l_shipmode char(10),l_comment varchar(44));
