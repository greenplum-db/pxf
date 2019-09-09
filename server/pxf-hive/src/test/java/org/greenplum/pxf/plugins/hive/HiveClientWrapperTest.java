package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.greenplum.pxf.api.model.Metadata;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class HiveClientWrapperTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Metadata.Item tblDesc;
    private HiveClientWrapper hiveClientWrapper;

    @Before
    public void setup() {
        HiveClientWrapper.HiveClientFactory factory = mock(HiveClientWrapper.HiveClientFactory.class);
        hiveClientWrapper = new HiveClientWrapper(factory);
    }

    @Test
    public void parseTableQualifiedNameNoDbName() {
        String name = "orphan";
        tblDesc = hiveClientWrapper.extractTableFromName(name);

        assertEquals("default", tblDesc.getPath());
        assertEquals(name, tblDesc.getName());
    }

    @Test
    public void parseTableQualifiedName() {
        String name = "not.orphan";
        tblDesc = hiveClientWrapper.extractTableFromName(name);

        assertEquals("not", tblDesc.getPath());
        assertEquals("orphan", tblDesc.getName());
    }

    @Test
    public void parseTableQualifiedNameEmpty() {
        String name = "";
        String errorMsg = "empty string is not a valid Hive table name. "
                + "Should be either <table_name> or <db_name.table_name>";

        parseTableQualifiedNameNegative(name, errorMsg, "empty string");

        name = null;
        parseTableQualifiedNameNegative(name, errorMsg, "null string");

        name = ".";
        errorMsg = surroundByQuotes(name) + " is not a valid Hive table name. "
                + "Should be either <table_name> or <db_name.table_name>";
        parseTableQualifiedNameNegative(name, errorMsg, "empty db and table names");

        name = " . ";
        errorMsg = surroundByQuotes(name) + " is not a valid Hive table name. "
                + "Should be either <table_name> or <db_name.table_name>";
        parseTableQualifiedNameNegative(name, errorMsg, "only white spaces in string");
    }

    @Test
    public void getDelimiterCode() {

        //Default delimiter code should be 44(comma)
        Integer delimiterCode = hiveClientWrapper.getDelimiterCode(null);
        char defaultDelim = ',';
        assertEquals((int) delimiterCode, (int) defaultDelim);

        //Some serdes use FIELD_DELIM key
        char expectedDelim = '%';
        StorageDescriptor sd = new StorageDescriptor();
        SerDeInfo si = new SerDeInfo();
        si.setParameters(Collections.singletonMap(serdeConstants.FIELD_DELIM, String.valueOf(expectedDelim)));
        sd.setSerdeInfo(si);
        delimiterCode = hiveClientWrapper.getDelimiterCode(sd);
        assertEquals((int) delimiterCode, (int) expectedDelim);

        //Some serdes use SERIALIZATION_FORMAT key
        sd = new StorageDescriptor();
        si = new SerDeInfo();
        si.setParameters(Collections.singletonMap(serdeConstants.SERIALIZATION_FORMAT, String.valueOf((int) expectedDelim)));
        sd.setSerdeInfo(si);
        delimiterCode = hiveClientWrapper.getDelimiterCode(sd);
        assertEquals((int) delimiterCode, (int) expectedDelim);
    }

    @Test
    public void parseTableQualifiedNameTooManyQualifiers() {
        String name = "too.many.parents";
        String errorMsg = surroundByQuotes(name) + " is not a valid Hive table name. "
                + "Should be either <table_name> or <db_name.table_name>";

        parseTableQualifiedNameNegative(name, errorMsg, "too many qualifiers");
    }

    @Test
    public void invalidTableName() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("\"t.r.o.u.b.l.e.m.a.k.e.r\" is not a valid Hive table name. Should be either <table_name> or <db_name.table_name>");

        IMetaStoreClient metaStoreClient = mock(IMetaStoreClient.class);
        hiveClientWrapper.extractTablesFromPattern(metaStoreClient, "t.r.o.u.b.l.e.m.a.k.e.r");
    }

    private void parseTableQualifiedNameNegative(String name, String errorMsg, String reason) {
        try {
            tblDesc = hiveClientWrapper.extractTableFromName(name);
            fail("test should fail because of " + reason);
        } catch (IllegalArgumentException e) {
            assertEquals(errorMsg, e.getMessage());
        }
    }

    private String surroundByQuotes(String str) {
        return "\"" + str + "\"";
    }

}