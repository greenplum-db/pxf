package org.greenplum.pxf.diagnostic;

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;

/**
 * Test class for regression tests.
 * The only thing this class does is to take received user data and
 * return it as the third column value on each readNextObject call.
 * The returned data has 4 columns delimited with DELIMITER property value.
 * First column - text, second column - int (counter), third column - bool, fourth column - text.
 */
public class UserDataVerifyAccessor extends BasePlugin implements Accessor {

    private String filter;
    private String userDelimiter;

    private int counter = 0;
    private char firstColumn = 'A';
    private static final String UNSUPPORTED_ERR_MESSAGE = "UserDataVerifyAccessor does not support write operation";

    @Override
    public boolean openForRead() {
        FilterVerifyFragmentMetadata metadata = context.getFragmentMetadata();
        filter = metadata.getFilter();
        userDelimiter = String.valueOf(context.getGreenplumCSV().getDelimiter());
        return true;
    }

    @Override
    public OneRow readNextObject() {

        // Termination rule
        if (counter >= 10) {
            return null;
        }

        // Generate tuple with user data value as last column.
        String data = firstColumn + userDelimiter // text
                    + counter + userDelimiter // integer
                    + (counter % 2 == 0) + userDelimiter // boolean
                    + (counter + "." + counter + 1) + userDelimiter // numeric, decimal, real, double precision
                    + (firstColumn + "" + firstColumn) + userDelimiter // text, varchar, bpchar
                    + filter;
        String key = Integer.toString(counter);

        counter++;
        firstColumn++;

        return new OneRow(key, data);
    }

    @Override
    public void closeForRead() {
    }

    @Override
    public boolean openForWrite() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
    }

    @Override
    public boolean writeNextObject(OneRow onerow) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
    }

    @Override
    public void closeForWrite() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
    }
}