package org.greenplum.pxf.plugins.hive.orc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.PxfRecordReaderImpl;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.ReaderImpl;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A specialized {@link ReaderImpl} that returns a {@link PxfRecordReaderImpl}
 * for the {@link #rowsOptions(Options)} method.
 */
public class PxfReaderImpl extends ReaderImpl
        implements Reader {

    private static final Logger LOG = LoggerFactory.getLogger(PxfReaderImpl.class);

    /**
     * Constructor that let's the user specify additional options.
     */
    public PxfReaderImpl(Path path, OrcFile.ReaderOptions options) throws IOException {
        super(path, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordReader rowsOptions(Options options) throws IOException {
        LOG.info("Reading ORC rows from {} with {}", path, options);
        return new PxfRecordReaderImpl(this, options);
    }
}
