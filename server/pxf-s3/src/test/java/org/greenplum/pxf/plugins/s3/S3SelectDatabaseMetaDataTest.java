package org.greenplum.pxf.plugins.s3;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class S3SelectDatabaseMetaDataTest {

    @Test
    public void testDefaults() {
        S3SelectDatabaseMetaData databaseMetaData = new S3SelectDatabaseMetaData();

        assertEquals("\"", databaseMetaData.getIdentifierQuoteString());
        assertEquals("S3 SELECT", databaseMetaData.getDatabaseProductName());
        assertEquals("", databaseMetaData.getExtraNameCharacters());
        assertTrue(databaseMetaData.supportsMixedCaseIdentifiers());
    }

}