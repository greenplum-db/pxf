package org.greenplum.pxf.automation.features.tpch;

public class LineItem {

    public static final String[] LINEITEM_SCHEMA = {
            "l_orderkey       BIGINT",
            "l_partkey        BIGINT",
            "l_suppkey        BIGINT",
            "l_linenumber     BIGINT",
            "l_quantity       DECIMAL(15,2)",
            "l_extendedprice  DECIMAL(15,2)",
            "l_discount       DECIMAL(15,2)",
            "l_tax            DECIMAL(15,2)",
            "l_returnflag     CHAR(1)",
            "l_linestatus     CHAR(1)",
            "l_shipdate       DATE",
            "l_commitdate     DATE",
            "l_receiptdate    DATE",
            "l_shipinstruct   CHAR(25)",
            "l_shipmode       CHAR(10)",
            "l_comment        VARCHAR(44)"
    };
}
