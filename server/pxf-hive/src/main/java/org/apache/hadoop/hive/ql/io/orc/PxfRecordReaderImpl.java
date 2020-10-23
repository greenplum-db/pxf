package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;

import java.io.IOException;

/**
 * This class fixes an issue introduced in ORC core library version 1.5.9
 * where the {@link org.apache.orc.TypeDescription#createColumn(TypeDescription.RowBatchVersion, int)}
 * method returns a {@link DateColumnVector} instead of a
 * {@link org.apache.hadoop.hive.ql.exec.vector.LongColumnVector}. The problem
 * with that change is that
 * {@link RecordReaderImpl#copyColumn(ColumnVector, ColumnVector, int, int)}
 * does not handle {@link DateColumnVector} objects. Because
 * {@link DateColumnVector} is a subclass of {@link org.apache.hadoop.hive.ql.exec.vector.LongColumnVector}
 * we can use the {@link #copyLongColumn(ColumnVector, ColumnVector, int, int)}
 * method to maintain the previous behavior of copyColumn.
 */
public class PxfRecordReaderImpl extends RecordReaderImpl
        implements RecordReader {

    /**
     * Constructs a PxfRecordReaderImpl given a file reader and options
     */
    public PxfRecordReaderImpl(ReaderImpl fileReader, Reader.Options options)
            throws IOException {
        super(fileReader, options);
    }

    /**
     * This method handles {@link DateColumnVector} object copying. For
     * other {@link ColumnVector} types it maintains the original functionality
     *
     * @param destination  the destination column vector
     * @param source       the source column vector
     * @param sourceOffset the offset in the source
     * @param length       the length of the copy
     */
    @Override
    void copyColumn(ColumnVector destination, ColumnVector source, int sourceOffset, int length) {
        if (source.getClass() == DateColumnVector.class) {
            copyLongColumn(destination, source, sourceOffset, length);
        } else {
            super.copyColumn(destination, source, sourceOffset, length);
        }
    }
}
