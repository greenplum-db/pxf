package org.greenplum.pxf.plugins.s3;

import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class S3SelectAccessorTest {

    @Test
    public void testGetInputSerializationDefaults() {
        RequestContext context = getDefaultRequestContext();

        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertNotNull(inputSerialization);
        assertNotNull(inputSerialization.getCsv());
        assertNull(inputSerialization.getCsv().getAllowQuotedRecordDelimiter());
        assertNull(inputSerialization.getCsv().getComments());
        assertNull(inputSerialization.getCsv().getFileHeaderInfo());
        assertEquals(',', inputSerialization.getCsv().getFieldDelimiter().charValue());
        assertEquals('"', inputSerialization.getCsv().getQuoteCharacter().charValue());
        assertEquals('"', inputSerialization.getCsv().getQuoteEscapeCharacter().charValue());
        assertEquals('\n', inputSerialization.getCsv().getRecordDelimiter().charValue());
        assertNull(inputSerialization.getJson());
        assertNull(inputSerialization.getParquet());
        assertEquals("NONE", inputSerialization.getCompressionType());
    }

    @Test
    public void testCompressionTypeGZIP() {
        RequestContext context = getDefaultRequestContext();
        context.addOption("COMPRESSION_CODEC", "GZIP");

        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertEquals("GZIP", inputSerialization.getCompressionType());
    }

    @Test
    public void testCompressionTypeBZIP2() {
        RequestContext context = getDefaultRequestContext();
        context.addOption("COMPRESSION_CODEC", "BZIP2");

        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertEquals("BZIP2", inputSerialization.getCompressionType());
    }

    @Test
    public void testParquetInputSerialization() {
        RequestContext context = getRequestContext("s3:parquet");
        context.setFormat("parquet");

        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertNotNull(inputSerialization.getParquet());
        assertNull(inputSerialization.getJson());
        assertNull(inputSerialization.getCsv());
    }

    @Test
    public void testJSONInputSerialization() {
        RequestContext context = getRequestContext("s3:json");
        context.setFormat("json");

        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertNotNull(inputSerialization.getJson());
        assertNull(inputSerialization.getCsv());
        assertNull(inputSerialization.getParquet());
    }

    @Test
    public void testJSONInputSerializationWithDocumentJsonType() {
        RequestContext context = getRequestContext("s3:json");
        context.setFormat("json");
        context.addOption("JSON-TYPE", "document");

        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertNotNull(inputSerialization.getJson());
        assertNull(inputSerialization.getCsv());
        assertNull(inputSerialization.getParquet());
        assertEquals("document", inputSerialization.getJson().getType());
    }

    @Test
    public void testJSONInputSerializationWithLinesJsonType() {
        RequestContext context = getRequestContext("s3:json");
        context.setFormat("json");
        context.addOption("JSON-TYPE", "lines");

        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertNotNull(inputSerialization.getJson());
        assertNull(inputSerialization.getCsv());
        assertNull(inputSerialization.getParquet());
        assertEquals("lines", inputSerialization.getJson().getType());
    }

    @Test
    public void testPipeDelimiter() {
        RequestContext context = getDefaultRequestContext();
        context.getGreenplumCSV().withDelimiter("|");

        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertEquals('|', inputSerialization.getCsv().getFieldDelimiter().charValue());
    }

    @Test
    public void testFileHeaderInfoIsIgnore() {
        RequestContext context = getDefaultRequestContext();
        context.addOption("FILE_HEADER", "IGNORE");
        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertEquals("IGNORE", inputSerialization.getCsv().getFileHeaderInfo());
    }

    @Test
    public void testFileHeaderInfoIsUse() {
        RequestContext context = getDefaultRequestContext();
        context.addOption("FILE_HEADER", "USE");
        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertEquals("USE", inputSerialization.getCsv().getFileHeaderInfo());
    }

    @Test
    public void testFileHeaderInfoIsNone() {
        RequestContext context = getDefaultRequestContext();
        context.addOption("FILE_HEADER", "NONE");
        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertEquals("NONE", inputSerialization.getCsv().getFileHeaderInfo());
    }

    @Test
    public void testQuoteEscapeCharacter() {
        RequestContext context = getDefaultRequestContext();
        context.addOption("ESCAPE", "\"");
        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertEquals('\"', inputSerialization.getCsv().getQuoteEscapeCharacter().charValue());
    }

    @Test
    public void testRecordDelimiter() {
        RequestContext context = getDefaultRequestContext();
        context.addOption("NEWLINE", "\n");
        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertEquals('\n', inputSerialization.getCsv().getRecordDelimiter().charValue());
    }

    @Test
    public void testQuoteCharacter() {
        RequestContext context = getDefaultRequestContext();
        context.addOption("QUOTE", "\"");
        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertEquals('"', inputSerialization.getCsv().getQuoteCharacter().charValue());
    }

    @Test
    public void testCorrectlyParsesDataSource() {
        RequestContext context = getDefaultRequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        context.setDataSource("s3a://my-bucket/my/s3/path/");
        context.setConfiguration(new Configuration());

        S3SelectAccessor accessor = new S3SelectAccessor();
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        SelectObjectContentRequest request = accessor.generateBaseCSVRequest(context);
        assertEquals("my-bucket", request.getBucketName());
        assertEquals("my/s3/path/", request.getKey());
    }

    @Test
    public void testCorrectlyParsesDataSourceWithNoKey() {
        RequestContext context = getDefaultRequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        context.setDataSource("s3a://my-bucket");
        context.setConfiguration(new Configuration());

        S3SelectAccessor accessor = new S3SelectAccessor();
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        SelectObjectContentRequest request = accessor.generateBaseCSVRequest(context);
        assertEquals("my-bucket", request.getBucketName());
        assertEquals("", request.getKey());
    }

    @Test
    public void testFailsToParseNullDataSource() {
        RequestContext context = new RequestContext();

        assertThrows(NullPointerException.class,
                () -> new S3SelectAccessor().generateBaseCSVRequest(context));
    }

    @Test
    public void testFailsOnOpenForWrite() {
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> new S3SelectAccessor().openForWrite());
        assertEquals("S3 Select accessor does not support write operation.", e.getMessage());
    }

    @Test
    public void testFailsOnWriteNextObject() {
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> new S3SelectAccessor().writeNextObject(new OneRow()));
        assertEquals("S3 Select accessor does not support write operation.", e.getMessage());
    }

    @Test
    public void testFailsOnCloseForWrite() {
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> new S3SelectAccessor().closeForWrite());
        assertEquals("S3 Select accessor does not support write operation.", e.getMessage());
    }

    private RequestContext getDefaultRequestContext() {
        return getRequestContext("s3:csv");
    }

    private RequestContext getRequestContext(String s) {
        RequestContext context = new RequestContext();
        context.setProfile(s);
        return context;
    }
}
