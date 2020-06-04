package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StreamingImageAccessorTest {
    private StreamingImageAccessor accessor;
    private List<BufferedImage> images;
    private List<String> paths;
    private List<String> correctPaths;
    private static final int NUM_IMAGES = 5;
    private static final String imageLocation = "/tmp/publicstage/pxf/StreamingImageAccessorTest";

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void setup() throws IOException {
        paths = new ArrayList<>();
        correctPaths = new ArrayList<>();
        List<String> fileNames = new ArrayList<>();
        images = ImageTestHelper.generateRandomImages(8, 4, NUM_IMAGES, fileNames, imageLocation);
        paths.add(imageLocation);
        paths.addAll(fileNames.stream().map(f -> f + ",0/2").collect(Collectors.toList()));
        correctPaths.addAll(paths.stream().skip(1).map(p -> imageLocation + "/" + p).collect(Collectors.toList()));
        accessor = new StreamingImageAccessor();
        RequestContext context = new RequestContext();
        context.setConfig("fakeConfig");
        context.setServerName("fakeServerName");
        context.setUser("fakeUser");
        context.setSegmentId(0);
        context.setTransactionId("testID");
        context.setProfileScheme("localfile");
        context.setDataSource(String.join("|", paths));
        accessor.initialize(context);
    }

    @Test
    public void testOpenForRead() throws Exception {
        assertNull(accessor.getPaths());

        assertTrue(accessor.openForRead());
        assertEquals(5, accessor.getPaths().size());
    }

    @Test
    public void testReadNextObject() throws Exception {
        assertTrue(accessor.openForRead());
        OneRow row = accessor.readNextObject();
        assertNotNull(row);
        assertTrue(row.getKey() instanceof List);
        assertTrue(row.getData() instanceof StreamingImageAccessor);
        int cnt = 0;
        for (Object path : (List<?>) row.getKey()) {
            assertEquals(correctPaths.get(cnt++), path);
        }
        assertEquals(NUM_IMAGES, cnt);
        assertNull(accessor.readNextObject());
    }

    @Test
    public void testReadNextImage() throws Exception {
        assertTrue(accessor.openForRead());

        OneRow row = accessor.readNextObject();

        StreamingImageAccessor passedAccessor = (StreamingImageAccessor) row.getData();
        for (BufferedImage image : images) {
            assertTrue(accessor.hasNext());
            assertImageEquals(image, passedAccessor.next());
        }
        assertFalse(accessor.hasNext());
        assertNull(passedAccessor.next());
    }

    private static void assertImageEquals(BufferedImage image, BufferedImage[] readImage) {
        int h = image.getHeight();
        int w = image.getWidth();
        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {
                assertEquals(image.getRGB(i, j), readImage[0].getRGB(i, j));
            }
        }
    }
}