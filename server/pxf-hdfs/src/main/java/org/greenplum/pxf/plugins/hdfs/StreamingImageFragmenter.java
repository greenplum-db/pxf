package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.greenplum.pxf.api.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.StreamingFragmenter;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class StreamingImageFragmenter extends HdfsMultiFileFragmenter implements StreamingFragmenter {
    private static final short FULL_PATH_COLUMN = 0;
    private static final short IMAGE_NAME_COLUMN = 1;
    private static final short PARENT_DIRECTORY_COLUMN = 2;
    private static final short ONE_HOT_ENCODING_COLUMN = 3;
    private static final short IMAGE_DIMENSIONS_COLUMN = 4;
    private static final short IMAGE_DATA_COLUMN = 5;
    private static final short NUM_IMAGE_COLUMNS = 6;
    private boolean fullPathColumnTypeIsScalar = false;
    private List<String> files = new ArrayList<>();
    private final List<Path> dirs = new ArrayList<>();
    private int currentDir = 0;
    private int currentFile = 0;
    private FileSystem fs;
    private final Set<String> labelSet = new HashSet<>();
    private final Map<String, String> labels = new HashMap<>();
    private String pathPrefix;

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);
        validateColumnTypes(context);
        if (fullPathColumnTypeIsScalar) {
            // ignore FILES_PER_FRAGMENT option if user hasn't set up table with arrays
            filesPerFragment = 1;
        }
    }

    @Override
    public List<Path> getDirs() {
        return dirs;
    }

    @Override
    public void open() throws Exception {
        if (!dirs.isEmpty()) {
            return;
        }
        String path = hcfsType.getDataUri(jobConf, context);
        pathPrefix = path.replaceAll("/$", "");
        fs = FileSystem.get(new URI(path), jobConf);
        getDirs(new Path(path));
        dirs.sort(Comparator.comparing(Path::toString));
        int cnt = 0;
        for (Object o : Arrays.stream(labelSet.toArray()).sorted().toArray()) {
            labels.put((String) o, cnt + "/" + dirs.size());
            LOG.debug("hot-encoding: dir name: {}, encoding: {}", o, cnt + "/" + dirs.size());
            cnt++;
        }
    }

    /**
     * Gets a batch of files and returns it as a comma-separated list within a single Fragment.
     */
    @Override
    public Fragment next() throws IOException {
        StringBuilder pathList = new StringBuilder();
        for (int i = 0; i < filesPerFragment; i++) {
            if (currentFile == files.size()) {
                getMoreFiles();
                if (currentFile == files.size() && currentDir == dirs.size()) {
                    break;
                }
            }
            if (pathList.length() == 0) {
                pathList.append(pathPrefix).append("|");
            }
            pathList.append(files.set(currentFile++, null)).append("|");
        }
        if (pathList.length() == 0) {
            return null;
        }
        pathList.setLength(pathList.length() - 1);

        return new Fragment(pathList.toString());
    }

    @Override
    public boolean hasNext() throws IOException {
        if (currentFile < files.size()) {
            return true;
        }
        if (currentDir < dirs.size()) {
            getMoreFiles();
        }
        return files.size() > 0 && currentFile < files.size();
    }

    private void getMoreFiles() throws IOException {
        currentFile = 0;
        files.clear();
        while (currentDir < dirs.size() && files.isEmpty()) {
            final Path dir = dirs.get(currentDir++);
            files = Arrays
                    .stream(fs.listStatus(dir))
                    .filter(file -> !file.isDirectory())
                    .map(file -> file.getPath().toUri().toString().replaceFirst(pathPrefix + "/", "") + "," + labels.get(file.getPath().getParent().getName()))
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    private void getDirs(Path path) throws IOException {
        boolean pathContainsRegularFiles = false;
        // this iterator only looks at one level of the directory tree
        RemoteIterator<FileStatus> iterator = fs.listStatusIterator(path);
        while (iterator.hasNext()) {
            FileStatus fileStatus = iterator.next();
            if (fileStatus.isDirectory()) {
                Path curPath = fileStatus.getPath();
                getDirs(curPath);
            } else if (!pathContainsRegularFiles) {
                pathContainsRegularFiles = true;
                labelSet.add(path.getName());
                dirs.add(path);
            }
        }
    }

    /**
     * Not implemented, see StreamingImageFragmenter#next() and StreamingImageFragmenter#hasNext()
     */
    @Override
    public List<Fragment> getFragments() {
        throw new UnsupportedOperationException("Operation getFragments is not supported, use next() and hasNext()");
    }

    @Override
    public FragmentStats getFragmentStats() {
        throw new UnsupportedOperationException("Operation getFragmentStats is not supported");
    }

    private void validateColumnTypes(RequestContext context) {
        if (context.getColumns() != NUM_IMAGE_COLUMNS) {
            throw new UnsupportedTypeException("image table must have exactly " + NUM_IMAGE_COLUMNS + " columns");
        }
        DataType fullPathColumnType = context.getColumn(FULL_PATH_COLUMN).getDataType();
        if (fullPathColumnType != DataType.TEXT && fullPathColumnType != DataType.TEXTARRAY) {
            throw new UnsupportedTypeException("first column of image table (image paths) must be TEXT or TEXT[]");
        }
        DataType imageNameColumnType = context.getColumn(IMAGE_NAME_COLUMN).getDataType();
        if (imageNameColumnType != DataType.TEXT && imageNameColumnType != DataType.TEXTARRAY) {
            throw new UnsupportedTypeException("second column of image table (image names) must be TEXT or TEXT[]");
        }
        DataType parentDirectoryColumnType = context.getColumn(PARENT_DIRECTORY_COLUMN).getDataType();
        if (parentDirectoryColumnType != DataType.TEXT && parentDirectoryColumnType != DataType.TEXTARRAY) {
            throw new UnsupportedTypeException("third column of image table (image parent directory) must be TEXT or TEXT[]");
        }
        DataType oneHotEncodingColumnType = context.getColumn(ONE_HOT_ENCODING_COLUMN).getDataType();
        if (oneHotEncodingColumnType != DataType.INT2ARRAY && oneHotEncodingColumnType != DataType.INT4ARRAY && oneHotEncodingColumnType != DataType.INT8ARRAY && oneHotEncodingColumnType != DataType.BYTEA) {
            throw new UnsupportedTypeException("fourth column of image table (image one hot encoding array) must be INT[] or BYTEA");
        }
        DataType imageDimensionsColumnType = context.getColumn(IMAGE_DIMENSIONS_COLUMN).getDataType();
        if (imageDimensionsColumnType != DataType.INT2ARRAY && imageDimensionsColumnType != DataType.INT4ARRAY && imageDimensionsColumnType != DataType.INT8ARRAY) {
            throw new UnsupportedTypeException("fifth column of image table (image dimensions array) must be INT[]");
        }
        DataType imageDataColumnType = context.getColumn(IMAGE_DATA_COLUMN).getDataType();
        if (imageDataColumnType != DataType.INT2ARRAY && imageDataColumnType != DataType.INT4ARRAY && imageDataColumnType != DataType.INT8ARRAY && imageDataColumnType != DataType.FLOAT4ARRAY && imageDataColumnType != DataType.BYTEA) {
            throw new UnsupportedTypeException("sixth column of image table (image data array) must be INT[] or BYTEA");
        }
        if (fullPathColumnType != imageNameColumnType || fullPathColumnType != parentDirectoryColumnType) {
            throw new UnsupportedTypeException("first, second, and third columns of image table must have the same type (TEXT or TEXT[])");
        }
        if (fullPathColumnType == DataType.TEXT) {
            fullPathColumnTypeIsScalar = true;
        }
    }
}
