package org.greenplum.pxf.plugins.hdfs;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ImageTestHelper {
    private static final int COLOR_MAX = 256;
    private static final Map<String, Integer> maskOffsets = new HashMap<String, Integer>() {{
        put("r", 16);
        put("g", 8);
        put("b", 0);
    }};

    public static List<BufferedImage> generateRandomImages(int w, int h, int numImages, List<String> fileNames, String imageLocation) throws IOException {
        List<BufferedImage> images = new ArrayList<>();
        if (fileNames == null) {
            fileNames = new ArrayList<>(); // will only be accessible here
        }
        for (int i = 0; i < numImages; i++) {
            fileNames.add("image" + i + ".png");
            images.add(new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB));
        }

        Random rand = new Random();
        for (int j = 0; j < numImages; j++) {
            for (int i = 0; i < w * h; i++) {
                final int r = rand.nextInt(COLOR_MAX) << maskOffsets.get("r");
                final int g = rand.nextInt(COLOR_MAX) << maskOffsets.get("g");
                final int b = rand.nextInt(COLOR_MAX) << maskOffsets.get("b");
                images.get(j).setRGB(i % w, i / w, r + g + b);
            }
        }

        if (imageLocation == null) {
            return images;
        }

        createDirectory(imageLocation);

        int cnt = 0;
        for (String location : fileNames) {
            final File file = new File(imageLocation + "/" + location);
            ImageIO.write(images.get(cnt++), "png", file);
        }
        return images;
    }

    public static List<BufferedImage> generateMonocolorImages(int w, int h, int[] values) throws IOException {
        List<BufferedImage> images = new ArrayList<>();

        for (int value : values) {
            BufferedImage image = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
            for (int i = 0; i < w * h; i++) {
                final int r = value % COLOR_MAX << maskOffsets.get("r");
                final int g = value % COLOR_MAX << maskOffsets.get("g");
                final int b = value % COLOR_MAX << maskOffsets.get("b");
                image.setRGB(i % w, i / w, r + g + b);
            }
            images.add(image);
        }
        return images;
    }

    private static void createDirectory(String dir) {
        File publicStageDir = new File(dir);
        if (!publicStageDir.exists()) {
            if (!publicStageDir.mkdirs()) {
                throw new RuntimeException(String.format("Could not create %s", dir));
            }
        }
    }
}
