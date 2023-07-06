package org.greenplum.pxf.automation.testplugin;

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
        String data = firstColumn + userDelimiter + counter + userDelimiter + (counter % 2 == 0) + userDelimiter + filter;
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
        throw new UnsupportedOperationException("openForWrite method is not implemented");
    }

    @Override
    public boolean writeNextObject(OneRow onerow) {
        throw new UnsupportedOperationException("writeNextObject method is not implemented");
    }

    @Override
    public void closeForWrite() {
        throw new UnsupportedOperationException("closeForWrite method is not implemented");
    }
}
