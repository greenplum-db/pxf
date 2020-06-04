package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StreamingImageFragmenterTest {
    StreamingImageFragmenter streamingHdfsFileFragmenter;
    private RequestContext context;
    private String path;
    private final List<ColumnDescriptor> textTupleDescription = new ArrayList<ColumnDescriptor>() {{
        add(0, new ColumnDescriptor("col0", DataType.TEXT.getOID(), 0, "TEXT", null));
        add(1, new ColumnDescriptor("col1", DataType.TEXT.getOID(), 1, "TEXT", null));
        add(2, new ColumnDescriptor("col2", DataType.TEXT.getOID(), 2, "TEXT", null));
        add(3, new ColumnDescriptor("col3", DataType.INT8ARRAY.getOID(), 3, "INT[]", null));
        add(4, new ColumnDescriptor("col4", DataType.INT8ARRAY.getOID(), 4, "INT[]", null));
        add(5, new ColumnDescriptor("col5", DataType.INT8ARRAY.getOID(), 5, "INT[]", null));
    }};
    private final List<ColumnDescriptor> textArrayTupleDescription = new ArrayList<ColumnDescriptor>() {{
        add(0, new ColumnDescriptor("col0", DataType.TEXTARRAY.getOID(), 0, "TEXT[]", null));
        add(1, new ColumnDescriptor("col1", DataType.TEXTARRAY.getOID(), 1, "TEXT[]", null));
        add(2, new ColumnDescriptor("col2", DataType.TEXTARRAY.getOID(), 2, "TEXT[]", null));
        add(3, new ColumnDescriptor("col3", DataType.INT8ARRAY.getOID(), 3, "INT[]", null));
        add(4, new ColumnDescriptor("col4", DataType.INT8ARRAY.getOID(), 4, "INT[]", null));
        add(5, new ColumnDescriptor("col5", DataType.INT8ARRAY.getOID(), 5, "INT[]", null));
    }};

    @Rule
    public TemporaryFolder tempFolder;

    @Before
    public void setup() throws IOException {
        context = new RequestContext();
        context.setConfig("default");
        context.setUser("user");
        context.setProfileScheme("localfile");
        context.setTupleDescription(textArrayTupleDescription);
        streamingHdfsFileFragmenter = new StreamingImageFragmenter();
        tempFolder = new TemporaryFolder();
        tempFolder.create();
        path = tempFolder.getRoot().toString();
        context.setDataSource(path);
        // important to test empty directories, but empty dirs are not tracked in git
        tempFolder.newFolder("empty_dir");
        tempFolder.newFolder("dir1", "nested_dir");
        tempFolder.newFolder("dir2", "empty_nested_dir");
        tempFolder.newFile("dir1/1.csv");
        tempFolder.newFile("dir1/2.csv");
        tempFolder.newFile("dir1/3.csv");
        tempFolder.newFile("dir2/1.csv");
        tempFolder.newFile("dir2/2.csv");
        tempFolder.newFile("dir2/3.csv");
        tempFolder.newFile("dir1/nested_dir/1.csv");
        tempFolder.newFile("dir1/nested_dir/2.csv");
        tempFolder.newFile("dir1/nested_dir/3.csv");
    }

    @Test
    public void testInitializeFilesPerFragmentNotGiven() {
        streamingHdfsFileFragmenter.initialize(context);

        assertEquals(1, streamingHdfsFileFragmenter.getFilesPerFragment());
    }


    @Test
    public void testInitializeFilesPerFragmentGivenWithTextColumn() {
        context.setTupleDescription(textTupleDescription);
        context.addOption(StreamingImageFragmenter.FILES_PER_FRAGMENT_OPTION_NAME, "100");
        streamingHdfsFileFragmenter.initialize(context);

        assertEquals(1, streamingHdfsFileFragmenter.getFilesPerFragment());
    }

    @Test
    public void testInitializeFilesPerFragmentGiven() {
        context.addOption(StreamingImageFragmenter.FILES_PER_FRAGMENT_OPTION_NAME, "100");
        streamingHdfsFileFragmenter.initialize(context);

        assertEquals(100, streamingHdfsFileFragmenter.getFilesPerFragment());
    }

    @Test
    public void testSearchForDirs() throws Exception {
        // dirs list needs to be sorted and include all levels of nesting
        tempFolder.newFolder("test", "dir3");
        tempFolder.newFolder("test", "empty_dir");
        tempFolder.newFolder("test", "dir2");
        tempFolder.newFolder("test", "dir1", "empty_dir", "foobar");
        tempFolder.newFolder("test", "a");
        tempFolder.newFile("test/0.csv");
        tempFolder.newFile("test/a/1.csv");
        tempFolder.newFile("test/dir1/2.csv");
        tempFolder.newFile("test/dir2/3.csv");
        tempFolder.newFile("test/dir3/4.csv");
        tempFolder.newFile("test/dir1/empty_dir/foobar/5.csv");
        context.setDataSource(path + "/test");
        initFragmenter();

        assertEquals(new ArrayList<Path>() {{
                         add(new Path("file://" + path + "/test"));
                         add(new Path("file://" + path + "/test/a"));
                         add(new Path("file://" + path + "/test/dir1"));
                         add(new Path("file://" + path + "/test/dir1/empty_dir/foobar"));
                         add(new Path("file://" + path + "/test/dir2"));
                         add(new Path("file://" + path + "/test/dir3"));
                     }},
                streamingHdfsFileFragmenter.getDirs());
    }

    @Test
    public void testNextAndHasNext_FilesPerFragmentNotGiven() throws Exception {
        initFragmenter();

        assertFragment(new Fragment("file://" + path + "|" + "dir1/1.csv,0/3"));
        assertFragment(new Fragment("file://" + path + "|" + "dir1/2.csv,0/3"));
        assertFragment(new Fragment("file://" + path + "|" + "dir1/3.csv,0/3"));
        assertFragment(new Fragment("file://" + path + "|" + "dir1/nested_dir/1.csv,2/3"));
        assertFragment(new Fragment("file://" + path + "|" + "dir1/nested_dir/2.csv,2/3"));
        assertFragment(new Fragment("file://" + path + "|" + "dir1/nested_dir/3.csv,2/3"));
        assertFragment(new Fragment("file://" + path + "|" + "dir2/1.csv,1/3"));
        assertFragment(new Fragment("file://" + path + "|" + "dir2/2.csv,1/3"));
        assertFragment(new Fragment("file://" + path + "|" + "dir2/3.csv,1/3"));
        assertNoMoreFragments();
    }

    @Test
    public void testNextAndHasNext_FilesInParentDir() throws Exception {
        tempFolder.newFolder("test");
        tempFolder.newFile("test/1.csv");
        tempFolder.newFile("test/2.csv");
        context.addOption(StreamingImageFragmenter.FILES_PER_FRAGMENT_OPTION_NAME, "10");
        context.setDataSource(path + "/test");
        initFragmenter();

        assertFragment(new Fragment("file://" + path + "/test|1.csv,0/1|" +
                "2.csv,0/1"));
        assertNoMoreFragments();
    }

    @Test
    public void testNextAndHasNext_LastDirIsNotEmpty() throws Exception {
        tempFolder.newFolder("test", "dir1", "empty_dir");
        tempFolder.newFolder("test", "dir2");
        tempFolder.newFile("test/dir1/1.csv");
        tempFolder.newFile("test/dir1/2.csv");
        tempFolder.newFile("test/dir1/3.csv");
        tempFolder.newFile("test/dir1/4.csv");
        tempFolder.newFile("test/dir1/5.csv");
        tempFolder.newFile("test/dir2/1.csv");
        tempFolder.newFile("test/dir2/2.csv");
        tempFolder.newFile("test/dir2/3.csv");
        tempFolder.newFile("test/dir2/4.csv");
        tempFolder.newFile("test/dir2/5.csv");
        context.addOption(StreamingImageFragmenter.FILES_PER_FRAGMENT_OPTION_NAME, "10");
        context.setDataSource(path + "/test");
        initFragmenter();

        assertFragment(new Fragment("file://" + path + "/test|"
                + "dir1/1.csv,0/2|"
                + "dir1/2.csv,0/2|"
                + "dir1/3.csv,0/2|"
                + "dir1/4.csv,0/2|"
                + "dir1/5.csv,0/2|"
                + "dir2/1.csv,1/2|"
                + "dir2/2.csv,1/2|"
                + "dir2/3.csv,1/2|"
                + "dir2/4.csv,1/2|"
                + "dir2/5.csv,1/2"
        ));
        assertNoMoreFragments();
    }

    @Test
    public void testNextAndHasNext_LargeFilesPerFragmentGiven() throws Exception {
        context.addOption(StreamingImageFragmenter.FILES_PER_FRAGMENT_OPTION_NAME, "100");
        initFragmenter();

        assertFragment(new Fragment("file://" + path + "|" +
                "dir1/1.csv,0/3|"
                + "dir1/2.csv,0/3|"
                + "dir1/3.csv,0/3|"
                + "dir1/nested_dir/1.csv,2/3|"
                + "dir1/nested_dir/2.csv,2/3|"
                + "dir1/nested_dir/3.csv,2/3|"
                + "dir2/1.csv,1/3|"
                + "dir2/2.csv,1/3|"
                + "dir2/3.csv,1/3"
        ));
        assertNoMoreFragments();
    }

    @Test
    public void testNextAndHasNext_SmallFilesPerFragmentGiven() throws Exception {
        context.addOption(StreamingImageFragmenter.FILES_PER_FRAGMENT_OPTION_NAME, "2");
        initFragmenter();

        assertFragment(new Fragment("file://" + path + "|"
                + "dir1/1.csv,0/3|"
                + "dir1/2.csv,0/3"
        ));
        assertFragment(new Fragment("file://" + path + "|"
                + "dir1/3.csv,0/3|"
                + "dir1/nested_dir/1.csv,2/3"
        ));
        assertFragment(new Fragment("file://" + path + "|"
                + "dir1/nested_dir/2.csv,2/3|"
                + "dir1/nested_dir/3.csv,2/3"
        ));
        assertFragment(new Fragment("file://" + path + "|"
                + "dir2/1.csv,1/3|"
                + "dir2/2.csv,1/3"
        ));
        assertFragment(new Fragment("file://" + path + "|"
                + "dir2/3.csv,1/3"
        ));
        assertNoMoreFragments();
    }

    private void initFragmenter() throws Exception {
        streamingHdfsFileFragmenter.initialize(context);
        streamingHdfsFileFragmenter.open();
    }

    private void assertFragment(Fragment correctFragment) throws IOException {
        assertTrue(streamingHdfsFileFragmenter.hasNext());
        Fragment fragment = streamingHdfsFileFragmenter.next();
        assertNotNull(fragment);
        assertEquals(correctFragment.getSourceName(), fragment.getSourceName());
    }

    private void assertNoMoreFragments() throws IOException {
        assertFalse(streamingHdfsFileFragmenter.hasNext());
        assertNull(streamingHdfsFileFragmenter.next());
    }
}
