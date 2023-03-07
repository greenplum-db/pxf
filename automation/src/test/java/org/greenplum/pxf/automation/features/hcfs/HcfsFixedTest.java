package org.greenplum.pxf.automation.features.hcfs;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

/**
 * Functional Test for fixed data format text files in HCFS
 * The dataset is based on a set of tests available in Greenplum
 * https://github.com/greenplum-db/gpdb/blob/main/contrib/formatter_fixedwidth/data/fixedwidth_small_correct.tbl
 */
public class HcfsFixedTest extends BaseFeature {

    private static final String[] SMALL_DATA_FIELDS = new String[]{
            "s1 char(10)",
            "s2 varchar(10)",
            "s3 text",
            "dt timestamp",
            "n1 smallint",
            "n2 integer",
            "n3 bigint",
            "n4 decimal",
            "n5 numeric",
            "n6 real",
            "n7 double precision"
    };

    private static final String[] SMALL_DATA_FORMATTER_OPTIONS = new String[]{
            "s1='10'",
            "s2='10'",
            "s3='10'",
            "dt='20'",
            "n1='5'",
            "n2='10'",
            "n3='10'",
            "n4='10'",
            "n5='10'",
            "n6='10'",
            "n7='15'"
    };

    private static final String fixedSmallCorrectFileName = "fixed_small_correct.txt";

    private String hdfsPath;
    private String resourcePath;

    /**
     * Prepare all components and all data flow (Hdfs to GPDB)
     */
    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/readableFixed/";

        // location of the data files
        String absolutePath = getClass().getClassLoader().getResource("data").getPath();
        resourcePath = absolutePath + "/fixed/";
    }

    /**
     * Before every method determine default hdfs data Path, default data, and
     * default external table structure. Each case change it according to it
     * needs.
     */
    @Override
    protected void beforeMethod() throws Exception {
        super.beforeMethod();
    }

    /**
     * Read fixed width formatted file from HCFS using *:fixed profile and fixedwidth_in format.
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readFixedUsingProfile() throws Exception {
        hdfs.copyFromLocal(resourcePath + fixedSmallCorrectFileName, hdfsPath + fixedSmallCorrectFileName);
        prepareReadableTable("fixed_in_small_correct", SMALL_DATA_FIELDS, SMALL_DATA_FORMATTER_OPTIONS,
                hdfsPath + fixedSmallCorrectFileName);
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hcfs.fixed.read.small_data_correct.runTest");
    }

    private void prepareReadableTable(String name, String[] fields, String[] formatterOptions, String path) {
        // default external table with common settings
        exTable = TableFactory.getPxfHcfsReadableTable(name, fields, path, hdfs.getBasePath(), "fixed");
        exTable.setFormatter("fixedwidth_in");
        exTable.setFormatterOptions(formatterOptions);
    }

}
