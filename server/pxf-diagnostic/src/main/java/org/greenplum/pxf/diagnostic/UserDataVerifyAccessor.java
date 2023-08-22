package org.greenplum.pxf.diagnostic;

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;

import java.util.StringJoiner;

/**
 * Test class for regression tests that generates rows of data and includes a filter provided by Greenplum.
 * The returned data has 7 columns delimited with DELIMITER property value: 6 columns of different types
 * and the last column with the value of the filter.
 */
public class UserDataVerifyAccessor extends BasePlugin implements Accessor {

    private static final CharSequence NULL = "";
    private String filter;
    private String userDelimiter;

    private int counter = 0;
    private char textColumn = 'A';
    private static final String UNSUPPORTED_ERR_MESSAGE = "UserDataVerifyAccessor does not support write operation";

    @Override
    public boolean openForRead() {

        // TODO allowlist the option
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
        String data = new StringJoiner(userDelimiter)
                .add(counter == 0 ? NULL : String.valueOf(textColumn))            // text
                .add(counter == 1 ? NULL : String.valueOf(counter))               // integer
                .add(counter == 2 ? NULL : String.valueOf(counter % 2 == 0))   // boolean
                .add(counter == 3 ? NULL : String.format("%1$d.%1$d1", counter))  // numeric, decimal, real, double precision
                .add(counter == 4 ? NULL : "" + textColumn + textColumn)          // bpchar
                .add(counter == 5 ? NULL : "" + textColumn + textColumn)          // varchar
                .add(filter)
                .toString();
        String key = Integer.toString(counter);

        counter++;
        textColumn++;

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
