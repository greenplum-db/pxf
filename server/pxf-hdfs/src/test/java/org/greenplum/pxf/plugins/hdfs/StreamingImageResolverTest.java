package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.ArrayField;
import org.greenplum.pxf.api.ArrayStreamingField;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.StreamingField;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;

@RunWith(MockitoJUnitRunner.class)
public class StreamingImageResolverTest {
    StreamingImageResolver resolver;
    List<String> paths;
    List<String> strippedPaths;
    List<String> parentDirs;
    List<String> fileNames;
    List<List<Integer>> oneHotArrays;
    byte[] oneHotArrays_bytea;
    List<Integer> dimensions;
    List<BufferedImage> images;
    List<byte[]> image_byteas;
    List<byte[]> image_float_byteas;
    private static final int NUM_IMAGES = 5;
    OneRow row;
    RequestContext context;
    @Mock
    StreamingImageAccessor accessor;
    List<String> imageStrings;
    List<String> imageFloatStrings;

    @Before
    public void setup() throws IOException, InterruptedException {
        context = new RequestContext();
        context.setTupleDescription(new ArrayList<ColumnDescriptor>() {{
            add(new ColumnDescriptor("paths", DataType.TEXTARRAY.getOID(), 0, "TEXT[]", null));
            add(new ColumnDescriptor("names", DataType.TEXTARRAY.getOID(), 1, "TEXT[]", null));
            add(new ColumnDescriptor("parent_dirs", DataType.TEXTARRAY.getOID(), 2, "TEXT[]", null));
            add(new ColumnDescriptor("one_hot_encodings", DataType.INT8ARRAY.getOID(), 3, "INT[]", null));
            add(new ColumnDescriptor("dimensions", DataType.INT8ARRAY.getOID(), 4, "INT[]", null));
            add(new ColumnDescriptor("images", DataType.INT8ARRAY.getOID(), 5, "INT[]", null));
        }});
        context.setConfig("fakeConfig");
        context.setServerName("fakeServerName");
        context.setUser("fakeUser");
        context.setSegmentId(0);
        context.setTransactionId("testID");
        context.setProfileScheme("localfile");
        resolver = new StreamingImageResolver();
        paths = new ArrayList<String>() {{
            add("/foo/parentDir1/image1.png,0/5");
            add("/foo/parentDir2/image2.png,1/5");
            add("/foo/parentDir3/image3.png,2/5");
            add("/foo/parentDir4/image4.png,3/5");
            add("/foo/parentDir5/image5.png,4/5");
        }};
        strippedPaths = new ArrayList<String>() {{
            add("/foo/parentDir1/image1.png");
            add("/foo/parentDir2/image2.png");
            add("/foo/parentDir3/image3.png");
            add("/foo/parentDir4/image4.png");
            add("/foo/parentDir5/image5.png");
        }};
        parentDirs = new ArrayList<String>() {{
            add("parentDir1");
            add("parentDir2");
            add("parentDir3");
            add("parentDir4");
            add("parentDir5");
        }};
        fileNames = new ArrayList<String>() {{
            add("image1.png");
            add("image2.png");
            add("image3.png");
            add("image4.png");
            add("image5.png");
        }};
        dimensions = new ArrayList<Integer>() {{
            add(5);
            add(2);
            add(2);
        }};
        oneHotArrays = new ArrayList<List<Integer>>() {{
            add(new ArrayList<Integer>() {{
                add(1);
                add(0);
                add(0);
                add(0);
                add(0);
            }});
            add(new ArrayList<Integer>() {{
                add(0);
                add(1);
                add(0);
                add(0);
                add(0);
            }});
            add(new ArrayList<Integer>() {{
                add(0);
                add(0);
                add(1);
                add(0);
                add(0);
            }});
            add(new ArrayList<Integer>() {{
                add(0);
                add(0);
                add(0);
                add(1);
                add(0);
            }});
            add(new ArrayList<Integer>() {{
                add(0);
                add(0);
                add(0);
                add(0);
                add(1);
            }});
        }};
        oneHotArrays_bytea = new byte[]{
                (byte) 1, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                (byte) 0, (byte) 1, (byte) 0, (byte) 0, (byte) 0,
                (byte) 0, (byte) 0, (byte) 1, (byte) 0, (byte) 0,
                (byte) 0, (byte) 0, (byte) 0, (byte) 1, (byte) 0,
                (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 1,
        };
        images = ImageTestHelper.generateMonocolorImages(2, 2, new int[]{1, 5, 10, 50, 100});
        imageStrings = new ArrayList<String>() {{
            add("{{{1,1,1},{1,1,1}},{{1,1,1},{1,1,1}}}");
            add("{{{5,5,5},{5,5,5}},{{5,5,5},{5,5,5}}}");
            add("{{{10,10,10},{10,10,10}},{{10,10,10},{10,10,10}}}");
            add("{{{50,50,50},{50,50,50}},{{50,50,50},{50,50,50}}}");
            add("{{{100,100,100},{100,100,100}},{{100,100,100},{100,100,100}}}");
        }};
        imageFloatStrings = new ArrayList<String>() {{
            add("{{{0.003921569,0.003921569,0.003921569},{0.003921569,0.003921569,0.003921569}},{{0.003921569,0.003921569,0.003921569},{0.003921569,0.003921569,0.003921569}}}");
            add("{{{0.019607844,0.019607844,0.019607844},{0.019607844,0.019607844,0.019607844}},{{0.019607844,0.019607844,0.019607844},{0.019607844,0.019607844,0.019607844}}}");
            add("{{{0.039215688,0.039215688,0.039215688},{0.039215688,0.039215688,0.039215688}},{{0.039215688,0.039215688,0.039215688},{0.039215688,0.039215688,0.039215688}}}");
            add("{{{0.19607843,0.19607843,0.19607843},{0.19607843,0.19607843,0.19607843}},{{0.19607843,0.19607843,0.19607843},{0.19607843,0.19607843,0.19607843}}}");
            add("{{{0.39215687,0.39215687,0.39215687},{0.39215687,0.39215687,0.39215687}},{{0.39215687,0.39215687,0.39215687},{0.39215687,0.39215687,0.39215687}}}");
        }};
        image_byteas = new ArrayList<byte[]>() {{
            add(new byte[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1});
            add(new byte[]{5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5});
            add(new byte[]{10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10});
            add(new byte[]{50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50});
            add(new byte[]{100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100});
        }};
        image_float_byteas = new ArrayList<byte[]>() {{
            for (int j : new ArrayList<Integer>() {{
                add(1);
                add(5);
                add(10);
                add(50);
                add(100);
            }}) {
                byte[] bytes = new byte[12 * 4];
                int intbits = Float.floatToIntBits((float) ((j < 0 ? j + 256 : j) / 255.0));
                for (int i = 0; i < 12; i++) {
                    bytes[i * 4] = (byte) (intbits >> 24);
                    bytes[i * 4 + 1] = (byte) (intbits >> 16);
                    bytes[i * 4 + 2] = (byte) (intbits >> 8);
                    bytes[i * 4 + 3] = (byte) (intbits);
                }
                add(bytes);
            }
        }};
        doAnswer(new Answer<Object>() {
            int cnt = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                return cnt == NUM_IMAGES ? null : new BufferedImage[]{images.get(cnt++)};
            }
        }).when(accessor).next();
        doAnswer(new Answer<Object>() {
            int cnt = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                return cnt++ != NUM_IMAGES;
            }
        }).when(accessor).hasNext();
        row = new OneRow(paths, accessor);
        resolver.initialize(context);
    }

    @Test
    public void testGetFields() throws InterruptedException {
        List<OneField> fields = resolver.getFields(row);

        assertEquals(6, fields.size());
        assertTrue(fields.get(0) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(0)).val instanceof List);
        assertTrue(fields.get(1) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(1)).val instanceof List);
        assertTrue(fields.get(2) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(2)).val instanceof List);
        assertTrue(fields.get(3) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(3)).val instanceof List);
        assertTrue(fields.get(4) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(4)).val instanceof List);
        assertTrue(fields.get(5) instanceof ArrayStreamingField);
        assertTrue(((ArrayStreamingField) fields.get(5)).getResolver() instanceof StreamingImageResolver);

        assertListEquals(strippedPaths, (List<?>) ((ArrayField) fields.get(0)).val);
        assertListEquals(fileNames, (List<?>) ((ArrayField) fields.get(1)).val);
        assertListEquals(parentDirs, (List<?>) ((ArrayField) fields.get(2)).val);
        assertListEquals(oneHotArrays, (List<?>) ((ArrayField) fields.get(3)).val);
        assertListEquals(dimensions, (List<?>) ((ArrayField) fields.get(4)).val);
        assertEquals(resolver, ((StreamingField) fields.get(5)).getResolver());
        assertImages(imageStrings);
    }

    @Test
    public void testGetFields_normalized() throws InterruptedException {
        context.addOption("NORMALIZE", "True");
        context.setTupleDescription(new ArrayList<ColumnDescriptor>() {{
            add(new ColumnDescriptor("paths", DataType.TEXTARRAY.getOID(), 0, "TEXT[]", null));
            add(new ColumnDescriptor("names", DataType.TEXTARRAY.getOID(), 1, "TEXT[]", null));
            add(new ColumnDescriptor("parent_dirs", DataType.TEXTARRAY.getOID(), 2, "TEXT[]", null));
            add(new ColumnDescriptor("one_hot_encodings", DataType.INT8ARRAY.getOID(), 3, "INT[]", null));
            add(new ColumnDescriptor("dimensions", DataType.INT8ARRAY.getOID(), 4, "INT[]", null));
            add(new ColumnDescriptor("images", DataType.FLOAT4ARRAY.getOID(), 5, "REAL[]", null));
        }});
        resolver.initialize(context);
        List<OneField> fields = resolver.getFields(row);

        assertEquals(6, fields.size());
        assertTrue(fields.get(0) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(0)).val instanceof List);
        assertTrue(fields.get(1) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(1)).val instanceof List);
        assertTrue(fields.get(2) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(2)).val instanceof List);
        assertTrue(fields.get(3) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(3)).val instanceof List);
        assertTrue(fields.get(4) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(4)).val instanceof List);
        assertTrue(fields.get(5) instanceof ArrayStreamingField);
        assertTrue(((ArrayStreamingField) fields.get(5)).getResolver() instanceof StreamingImageResolver);

        assertListEquals(strippedPaths, (List<?>) ((ArrayField) fields.get(0)).val);
        assertListEquals(fileNames, (List<?>) ((ArrayField) fields.get(1)).val);
        assertListEquals(parentDirs, (List<?>) ((ArrayField) fields.get(2)).val);
        assertListEquals(oneHotArrays, (List<?>) ((ArrayField) fields.get(3)).val);
        assertListEquals(dimensions, (List<?>) ((ArrayField) fields.get(4)).val);
        assertEquals(resolver, ((StreamingField) fields.get(5)).getResolver());
        assertImages(imageFloatStrings);
    }

    @Test
    public void testGetFields_scalarFields() throws InterruptedException {
        context.setTupleDescription(new ArrayList<ColumnDescriptor>() {{
            add(new ColumnDescriptor("paths", DataType.TEXT.getOID(), 0, "TEXT", null));
            add(new ColumnDescriptor("names", DataType.TEXT.getOID(), 1, "TEXT", null));
            add(new ColumnDescriptor("parent_dirs", DataType.TEXT.getOID(), 2, "TEXT", null));
            add(new ColumnDescriptor("one_hot_encodings", DataType.INT8ARRAY.getOID(), 3, "INT[]", null));
            add(new ColumnDescriptor("dimensions", DataType.INT8ARRAY.getOID(), 4, "INT[]", null));
            add(new ColumnDescriptor("images", DataType.INT8ARRAY.getOID(), 5, "INT[]", null));
        }});
        resolver.initialize(context);
        List<OneField> fields = resolver.getFields(row);

        assertEquals(6, fields.size());
        assertNotNull(fields.get(0));
        assertTrue((fields.get(0)).val instanceof String);
        assertNotNull(fields.get(1));
        assertTrue((fields.get(1)).val instanceof String);
        assertNotNull(fields.get(2));
        assertTrue((fields.get(2)).val instanceof String);
        assertTrue(fields.get(3) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(3)).val instanceof List);
        assertTrue(fields.get(4) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(4)).val instanceof List);
        assertTrue(fields.get(5) instanceof StreamingField);
        assertTrue(((StreamingField) fields.get(5)).getResolver() instanceof StreamingImageResolver);

        assertEquals(strippedPaths.get(0), fields.get(0).val);
        assertEquals(fileNames.get(0), fields.get(1).val);
        assertEquals(parentDirs.get(0), fields.get(2).val);
        assertListEquals(oneHotArrays.get(0), (List<?>) ((ArrayField) fields.get(3)).val);
        assertListEquals(dimensions.subList(1, dimensions.size()), (List<?>) ((ArrayField) fields.get(4)).val);
        assertEquals(resolver, ((StreamingField) fields.get(5)).getResolver());
        assertImages(imageStrings);
    }

    @Test
    public void testGetFields_scalarFields_bytea() throws InterruptedException {
        context.setTupleDescription(new ArrayList<ColumnDescriptor>() {{
            add(new ColumnDescriptor("paths", DataType.TEXT.getOID(), 0, "TEXT", null));
            add(new ColumnDescriptor("names", DataType.TEXT.getOID(), 1, "TEXT", null));
            add(new ColumnDescriptor("parent_dirs", DataType.TEXT.getOID(), 2, "TEXT", null));
            add(new ColumnDescriptor("one_hot_encodings", DataType.BYTEA.getOID(), 3, "BYTEA", null));
            add(new ColumnDescriptor("dimensions", DataType.INT8ARRAY.getOID(), 4, "INT[]", null));
            add(new ColumnDescriptor("images", DataType.BYTEA.getOID(), 5, "BYTEA", null));
        }});
        resolver.initialize(context);
        List<OneField> fields = resolver.getFields(row);

        assertEquals(6, fields.size());
        assertNotNull(fields.get(0));
        assertTrue((fields.get(0)).val instanceof String);
        assertNotNull(fields.get(1));
        assertTrue((fields.get(1)).val instanceof String);
        assertNotNull(fields.get(2));
        assertTrue((fields.get(2)).val instanceof String);
        assertNotNull(fields.get(3));
        assertTrue(fields.get(3).val instanceof byte[]);
        assertTrue(fields.get(4) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(4)).val instanceof List);
        assertTrue(fields.get(5) instanceof StreamingField);
        assertTrue(((StreamingField) fields.get(5)).getResolver() instanceof StreamingImageResolver);

        assertEquals(strippedPaths.get(0), fields.get(0).val);
        assertEquals(fileNames.get(0), fields.get(1).val);
        assertEquals(parentDirs.get(0), fields.get(2).val);
        assertArrayEquals(Arrays.copyOfRange(oneHotArrays_bytea, 0, 5), (byte[]) fields.get(3).val);
        assertListEquals(dimensions.subList(1, dimensions.size()), (List<?>) ((ArrayField) fields.get(4)).val);
        assertEquals(resolver, ((StreamingField) fields.get(5)).getResolver());
        assertImagesBytea(image_byteas);
    }

    @Test
    public void testGetFields_scalarFields_Normalized_bytea() throws InterruptedException {
        context.addOption("NORMALIZE", "True");
        context.setTupleDescription(new ArrayList<ColumnDescriptor>() {{
            add(new ColumnDescriptor("paths", DataType.TEXT.getOID(), 0, "TEXT", null));
            add(new ColumnDescriptor("names", DataType.TEXT.getOID(), 1, "TEXT", null));
            add(new ColumnDescriptor("parent_dirs", DataType.TEXT.getOID(), 2, "TEXT", null));
            add(new ColumnDescriptor("one_hot_encodings", DataType.BYTEA.getOID(), 3, "BYTEA", null));
            add(new ColumnDescriptor("dimensions", DataType.INT8ARRAY.getOID(), 4, "INT[]", null));
            add(new ColumnDescriptor("images", DataType.BYTEA.getOID(), 5, "BYTEA", null));
        }});
        resolver.initialize(context);
        List<OneField> fields = resolver.getFields(row);

        assertEquals(6, fields.size());
        assertNotNull(fields.get(0));
        assertTrue((fields.get(0)).val instanceof String);
        assertNotNull(fields.get(1));
        assertTrue((fields.get(1)).val instanceof String);
        assertNotNull(fields.get(2));
        assertTrue((fields.get(2)).val instanceof String);
        assertNotNull(fields.get(3));
        assertTrue(fields.get(3).val instanceof byte[]);
        assertTrue(fields.get(4) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(4)).val instanceof List);
        assertTrue(fields.get(5) instanceof StreamingField);
        assertTrue(((StreamingField) fields.get(5)).getResolver() instanceof StreamingImageResolver);

        assertEquals(strippedPaths.get(0), fields.get(0).val);
        assertEquals(fileNames.get(0), fields.get(1).val);
        assertEquals(parentDirs.get(0), fields.get(2).val);
        assertArrayEquals(Arrays.copyOfRange(oneHotArrays_bytea, 0, 5), (byte[]) fields.get(3).val);
        assertListEquals(dimensions.subList(1, dimensions.size()), (List<?>) ((ArrayField) fields.get(4)).val);
        assertEquals(resolver, ((StreamingField) fields.get(5)).getResolver());
        assertImagesBytea(image_float_byteas);
    }

    @Test
    public void testGetFields_byteArray() throws InterruptedException {
        context.setTupleDescription(new ArrayList<ColumnDescriptor>() {{
            add(new ColumnDescriptor("paths", DataType.TEXTARRAY.getOID(), 0, "TEXT[]", null));
            add(new ColumnDescriptor("names", DataType.TEXTARRAY.getOID(), 1, "TEXT[]", null));
            add(new ColumnDescriptor("parent_dirs", DataType.TEXTARRAY.getOID(), 2, "TEXT[]", null));
            add(new ColumnDescriptor("one_hot_encodings", DataType.BYTEA.getOID(), 3, "BYTEA", null));
            add(new ColumnDescriptor("dimensions", DataType.INT8ARRAY.getOID(), 4, "INT[]", null));
            add(new ColumnDescriptor("images", DataType.BYTEA.getOID(), 5, "BYTEA", null));
        }});
        resolver.initialize(context);
        List<OneField> fields = resolver.getFields(row);

        assertEquals(6, fields.size());
        assertTrue(fields.get(0) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(0)).val instanceof List);
        assertTrue(fields.get(1) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(1)).val instanceof List);
        assertTrue(fields.get(2) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(2)).val instanceof List);
        assertNotNull(fields.get(3));
        assertTrue((fields.get(3)).val instanceof byte[]);
        assertTrue(fields.get(4) instanceof ArrayField);
        assertTrue(((ArrayField) fields.get(4)).val instanceof List);
        assertTrue(fields.get(5) instanceof StreamingField);
        assertTrue(((StreamingField) fields.get(5)).getResolver() instanceof StreamingImageResolver);

        assertListEquals(strippedPaths, (List<?>) ((ArrayField) fields.get(0)).val);
        assertListEquals(fileNames, (List<?>) ((ArrayField) fields.get(1)).val);
        assertListEquals(parentDirs, (List<?>) ((ArrayField) fields.get(2)).val);
        assertArrayEquals(oneHotArrays_bytea, (byte[]) fields.get(3).val);
        assertListEquals(dimensions, (List<?>) ((ArrayField) fields.get(4)).val);
        assertEquals(resolver, ((StreamingField) fields.get(5)).getResolver());
        assertImagesBytea(image_byteas);
    }

    @Test
    public void testNextAndHasNext() throws InterruptedException {
        resolver.getFields(row);
        for (String image : imageStrings) {
            assertTrue(resolver.hasNext());
            assertEquals(image, resolver.next());
        }
        assertFalse(resolver.hasNext());
        // assertNull(resolver.next());
    }

    private void assertListEquals(List<?> correct, List<?> val) {
        int cnt = 0;
        for (Object c : correct) {
            assertEquals(c, val.get(cnt++));
        }
    }

    private void assertImagesBytea(List<byte[]> byteas) throws InterruptedException {
        int cnt = 0;
        while (resolver.hasNext()) {
            Object o = resolver.next();
            if (cnt == NUM_IMAGES) {
                assertNull(o);
                return;
            } else {
                assertTrue(o instanceof byte[]);
                assertArrayEquals(byteas.get(cnt++), (byte[]) o);
            }
        }
    }

    private void assertImages(List<String> strings) throws InterruptedException {
        int cnt = 0;
        while (resolver.hasNext()) {
            Object o = resolver.next();
            if (cnt == NUM_IMAGES) {
                assertNull(o);
                return;
            } else {
                assertEquals(strings.get(cnt++), o);
            }
        }
    }
}
