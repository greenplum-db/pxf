package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.ArrayField;
import org.greenplum.pxf.api.ArrayStreamingField;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.StreamingField;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.StreamingResolver;

import java.awt.image.BufferedImage;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This implementation of StreamingResolver works with the StreamingImageAccessor to
 * fetch and encode images into a string, one by one, placing multiple images into a single
 * field.
 * <p>
 * It hands off a reference to itself in a ArrayStreamingField so that the field can be used to
 * call back to the StreamingImageResolver in the BridgeOutputBuilder class. The resolver in turn
 * calls back to the StreamingImageAccessor to fetch images when needed.
 */
@SuppressWarnings("unchecked")
public class StreamingImageResolver extends BasePlugin implements StreamingResolver {
    private static final int IMAGE_FULL_PATH_COLUMN = 0;
    private static final int IMAGE_ONE_HOT_ENCODING_COLUMN = 3;
    private static final int IMAGE_DATA_COLUMN = 5;
    private StreamingImageAccessor accessor;
    private int currentImage = 0, currentThread = 0, numImages, w, h;
    private static final int INTENSITIES = 256, NUM_COL = 3;
    // cache of strings for RGB arrays going to Greenplum
    private static final String[] rIntegers = new String[INTENSITIES];
    private static final String[] gIntegers = new String[INTENSITIES];
    private static final String[] bIntegers = new String[INTENSITIES];
    private static final String[] rFloats = new String[INTENSITIES];
    private static final String[] gFloats = new String[INTENSITIES];
    private static final String[] bFloats = new String[INTENSITIES];
    private boolean normalize;
    private static final String NORMALIZE_OPTION_NAME = "NORMALIZE";
    private DataType imageFullPathColumnType;
    private DataType imageColumnType;
    private DataType imageOneHotEncodingColumnType;
    private BufferedImage[] currentImages;
    private Thread[] threads;
    private Object[] imageArrays;
    List<String> paths;

    static {
        String intStr;
        String floatStr;
        for (int i = 0; i < INTENSITIES; i++) {
            intStr = String.valueOf(i);
            rIntegers[i] = "{" + intStr;
            gIntegers[i] = "," + intStr + ",";
            bIntegers[i] = intStr + "}";
            floatStr = String.valueOf((float) (i / 255.0));
            rFloats[i] = "{" + floatStr;
            gFloats[i] = "," + floatStr + ",";
            bFloats[i] = floatStr + "}";
        }
    }

    /**
     * Returns Postgres-style arrays with full paths, parent directories, and names
     * of image files.
     */
    @Override
    public List<OneField> getFields(OneRow row) throws InterruptedException {
        imageFullPathColumnType = context.getColumn(IMAGE_FULL_PATH_COLUMN).getDataType();
        imageColumnType = context.getColumn(IMAGE_DATA_COLUMN).getDataType();
        imageOneHotEncodingColumnType = context.getColumn(IMAGE_ONE_HOT_ENCODING_COLUMN).getDataType();
        normalize = context.getOption(NORMALIZE_OPTION_NAME, "false").toLowerCase().equals("true");

        paths = (ArrayList<String>) row.getKey();
        accessor = (StreamingImageAccessor) row.getData();
        List<String> fullPaths = new ArrayList<>();
        List<String> parentDirs = new ArrayList<>();
        List<String> fileNames = new ArrayList<>();
        List<List<Integer>> oneHotArrays = new ArrayList<>();

        for (String pathString : paths) {
            String oneHotData = pathString.split(",")[1];
            List<Integer> oneHotIndexes = Arrays.stream(oneHotData.split("/"))
                    .map(Integer::parseInt)
                    .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
            List<Integer> oneHotArray = new ArrayList<>(Collections.nCopies(oneHotIndexes.get(1), 0));
            oneHotArray.set(oneHotIndexes.get(0), 1);
            pathString = pathString.split(",")[0];
            URI uri = URI.create(pathString);
            Path path = Paths.get(uri.getPath());

            fullPaths.add(uri.getPath());
            parentDirs.add(path.getParent().getFileName().toString());
            fileNames.add(path.getFileName().toString());
            oneHotArrays.add(oneHotArray);
        }

        numImages = fileNames.size();
        // get the first image since we need the width and height early
        getNextImages();
        w = currentImages[0].getWidth();
        h = currentImages[0].getHeight();
        LOG.debug("Image size {}w {}h", w, h);

        currentThread = currentImages.length;
        threads = new Thread[currentImages.length];
        imageArrays = new Object[currentImages.length];

        return new ArrayList<OneField>() {
            {
                List<Integer> imageDimensions = new ArrayList<>();
                if (imageFullPathColumnType == DataType.TEXT) {
                    add(new OneField(DataType.TEXT.getOID(), fullPaths.get(0)));
                    add(new OneField(DataType.TEXT.getOID(), fileNames.get(0)));
                    add(new OneField(DataType.TEXT.getOID(), parentDirs.get(0)));
                    if (imageOneHotEncodingColumnType == DataType.BYTEA) {
                        byte[] oneHotArrays_bytea = new byte[oneHotArrays.get(0).size()];
                        for (int i = 0; i < oneHotArrays.get(0).size(); i++) {
                            oneHotArrays_bytea[i] = (byte) (oneHotArrays.get(0).get(i) & 0xff);
                        }
                        add(new OneField((DataType.BYTEA.getOID()), oneHotArrays_bytea));
                    } else {
                        add(new ArrayField(DataType.INT8ARRAY.getOID(), oneHotArrays.get(0)));
                    }
                } else {
                    add(new ArrayField(DataType.TEXTARRAY.getOID(), fullPaths));
                    add(new ArrayField(DataType.TEXTARRAY.getOID(), fileNames));
                    add(new ArrayField(DataType.TEXTARRAY.getOID(), parentDirs));
                    if (imageOneHotEncodingColumnType == DataType.BYTEA) {
                        byte[] oneHotArrays_bytea = new byte[oneHotArrays.size() * oneHotArrays.get(0).size()];
                        for (int i = 0; i < oneHotArrays.size() * oneHotArrays.get(0).size(); i++) {
                            List<Integer> current = oneHotArrays.get(i / oneHotArrays.get(0).size());
                            oneHotArrays_bytea[i] = (byte) (current.get(i % oneHotArrays.get(0).size()) & 0xff);
                        }
                        add(new OneField(DataType.BYTEA.getOID(), oneHotArrays_bytea));
                    } else {
                        add(new ArrayField(DataType.INT8ARRAY.getOID(), oneHotArrays));
                    }
                    imageDimensions.add(numImages);
                }
                imageDimensions.add(h);
                imageDimensions.add(w);
                imageDimensions.add(NUM_COL);

                add(new ArrayField(DataType.INT8ARRAY.getOID(), imageDimensions));
                if (imageColumnType == DataType.BYTEA) {
                    add(new StreamingField(DataType.BYTEA.getOID(), StreamingImageResolver.this));
                } else {
                    if (imageFullPathColumnType == DataType.TEXT) {
                        add(new StreamingField(DataType.INT8ARRAY.getOID(), StreamingImageResolver.this));
                    } else {
                        add(new ArrayStreamingField(StreamingImageResolver.this));
                    }
                }
            }
        };
    }

    private void getNextImages() throws InterruptedException {
        currentImages = accessor.next();
    }

    @Override
    public boolean hasNext() {
        return currentImage < numImages;
    }

    /**
     * Returns Postgres-style multi-dimensional int array or Postgres BYTEA, piece by piece. Each
     * time this method is called it returns another image, where multiple images
     * will end up in the same tuple.
     */
    @Override
    public Object next() throws InterruptedException {
        if (currentThread == imageArrays.length) {
            for (int i = 0; i < currentImages.length; i++) {
                threads[i] = new Thread(new ProcessImageRunnable(i));
                threads[i].start();
            }
            for (int i = 0; i < currentImages.length; i++) {
                threads[i].join();
            }
            currentThread = 0;
            getNextImages();
        }

        currentImage++;
        return imageArrays[currentThread++];
    }

    class ProcessImageRunnable implements Runnable {
        private final int cnt;

        public ProcessImageRunnable(int cnt) {
            this.cnt = cnt;
        }

        private byte[] imageToByteArray(BufferedImage image) {
            byte[] bytea = new byte[w * h * NUM_COL];
            int[] shifts = new int[]{16, 8, 0};
            int cnt = 0;
            for (int pixel : image.getRGB(0, 0, w, h, null, 0, w)) {
                for (int shift : shifts) {
                    bytea[cnt++] = (byte) (pixel >> shift);
                }
            }
            return bytea;
        }

        private byte[] imageToNormalizedByteArray(BufferedImage image) {
            byte[] bytea = new byte[4 * w * h * NUM_COL];
            int[] shifts = new int[]{24, 16, 8, 0};
            int cnt = 0;
            for (int pixel : image.getRGB(0, 0, w, h, null, 0, w)) {
                byte[] rgb = new byte[]{(byte) (pixel >> 16), (byte) (pixel >> 8), (byte) (pixel)};
                for (byte color : rgb) {
                    int bits = Float.floatToIntBits((float) ((color < 0 ? color + 256 : color) / 255.0));
                    for (int shift : shifts) {
                        bytea[cnt++] = (byte) (bits >> shift);
                    }
                }
            }
            return bytea;
        }

        private String imageToPostgresArray(BufferedImage image) {
            StringBuilder sb;
            // avoid arrayCopy() in sb.append() by pre-calculating max image size
            sb = new StringBuilder();
            LOG.debug("Image length: {}, cap: {}", sb.length(), sb.capacity());
            processImage(sb, image, w, h);
            LOG.debug("Image length: {}, cap: {}", sb.length(), sb.capacity());
            return sb.toString();
        }

        @Override
        public void run() {
            if (imageColumnType == DataType.BYTEA) {
                imageArrays[cnt] = normalize ? imageToNormalizedByteArray(currentImages[cnt]) : imageToByteArray(currentImages[cnt]);
            } else {
                imageArrays[cnt] = imageToPostgresArray(currentImages[cnt]);
            }
        }
    }

    private void processImage(StringBuilder sb, BufferedImage image, int w, int h) {
        if (image == null) {
            return;
        }
        String[] r = normalize ? rFloats : rIntegers;
        String[] g = normalize ? gFloats : gIntegers;
        String[] b = normalize ? bFloats : bIntegers;
        sb.append("{{");
        int cnt = 0;
        for (int pixel : image.getRGB(0, 0, w, h, null, 0, w)) {
            sb.append(r[(pixel >> 16) & 0xff]).append(g[(pixel >> 8) & 0xff]).append(b[pixel & 0xff]).append(",");
            if (++cnt % w == 0) {
                sb.setLength(sb.length() - 1);
                sb.append("},{");
            }
        }
        sb.setLength(sb.length() - 2);
        sb.append("}");
    }

    /**
     * Constructs and sets the fields of a {@link OneRow}.
     *
     * @param record list of {@link OneField}
     * @return the constructed {@link OneRow}
     */
    @Override
    public OneRow setFields(List<OneField> record) {
        throw new UnsupportedOperationException();
    }
}
