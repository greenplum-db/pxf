package org.greenplum.pxf.plugins.s3;

import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class S3SelectAccessorTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testGetInputSerializationDefaults() {
        RequestContext context = getDefaultRequestContext();

        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertNotNull(inputSerialization);
        assertNotNull(inputSerialization.getCsv());
        assertNull(inputSerialization.getCsv().getAllowQuotedRecordDelimiter());
        assertNull(inputSerialization.getCsv().getComments());
        assertNull(inputSerialization.getCsv().getFieldDelimiter());
        assertNull(inputSerialization.getCsv().getFileHeaderInfo());
        assertNull(inputSerialization.getCsv().getQuoteCharacter());
        assertNull(inputSerialization.getCsv().getQuoteEscapeCharacter());
        assertNull(inputSerialization.getCsv().getRecordDelimiter());
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
        context.addOption("DELIMITER", "|");

        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertEquals(new Character('|'), inputSerialization.getCsv().getFieldDelimiter());
    }

    @Test
    public void testFileHeaderInfoNotProvided() {
        RequestContext context = getDefaultRequestContext();
        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertNull(inputSerialization.getCsv().getFileHeaderInfo());
    }

    @Test
    public void testFileHeaderInfoIsIgnore() {
        RequestContext context = getDefaultRequestContext();
        context.addOption("HEADER", "IGNORE");
        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertEquals("IGNORE", inputSerialization.getCsv().getFileHeaderInfo());
    }

    @Test
    public void testFileHeaderInfoIsUse() {
        RequestContext context = getDefaultRequestContext();
        context.addOption("HEADER", "USE");
        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertEquals("USE", inputSerialization.getCsv().getFileHeaderInfo());
    }

    @Test
    public void testFileHeaderInfoIsNone() {
        RequestContext context = getDefaultRequestContext();
        context.addOption("HEADER", "NONE");
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
        assertEquals(new Character('\"'), inputSerialization.getCsv().getQuoteEscapeCharacter());
    }

    @Test
    public void testRecordDelimiter() {
        RequestContext context = getDefaultRequestContext();
        context.addOption("NEWLINE", "\n");
        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertEquals(new Character('\n'), inputSerialization.getCsv().getRecordDelimiter());
    }

    @Test
    public void testQuoteCharacter() {
        RequestContext context = getDefaultRequestContext();
        context.addOption("QUOTE", "\"");
        InputSerialization inputSerialization =
                new S3SelectAccessor().getInputSerialization(context);
        assertEquals(new Character('"'), inputSerialization.getCsv().getQuoteCharacter());
    }

    @Test
    public void testCorrectlyParsesDataSource() {
        RequestContext context = getDefaultRequestContext();
        context.setDataSource("s3a://my-bucket/my/s3/path/");

        S3SelectAccessor accessor = new S3SelectAccessor();
        accessor.initialize(context);
        SelectObjectContentRequest request = accessor.generateBaseCSVRequest(context);
        assertEquals("my-bucket", request.getBucketName());
        assertEquals("my/s3/path/", request.getKey());
    }

    @Test
    public void testCorrectlyParsesDataSourceWithNoKey() {
        RequestContext context = getDefaultRequestContext();
        context.setDataSource("s3a://my-bucket");

        S3SelectAccessor accessor = new S3SelectAccessor();
        accessor.initialize(context);
        SelectObjectContentRequest request = accessor.generateBaseCSVRequest(context);
        assertEquals("my-bucket", request.getBucketName());
    }

    @Test
    public void testFailsToParseNullDataSource() {
        thrown.expect(NullPointerException.class);

        RequestContext context = new RequestContext();
        new S3SelectAccessor().generateBaseCSVRequest(context);
    }

    @Test
    public void testFailsOnOpenForWrite() {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("S3 Select does not support writing");
        new S3SelectAccessor().openForWrite();
    }

    @Test
    public void testFailsOnWriteNextObject() {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("S3 Select does not support writing");
        new S3SelectAccessor().writeNextObject(new OneRow());
    }

    @Test
    public void testFailsOnCloseForWrite() {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("S3 Select does not support writing");
        new S3SelectAccessor().closeForWrite();
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
