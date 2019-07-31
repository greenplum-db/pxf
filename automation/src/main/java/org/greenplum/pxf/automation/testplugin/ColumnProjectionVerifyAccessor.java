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
public class ColumnProjectionVerifyAccessor extends BasePlugin implements Accessor {
    private String userData;
    private String userDelimiter;

    private int counter = 0;
    private char firstColumn = 'A';

    @Override
    public boolean openForRead() throws Exception {

        // TODO whitelist the option
        userData = new String(context.getFragmentUserData());
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
        String data = getData();
        String key = Integer.toString(counter);

        counter++;
        firstColumn++;

        return new OneRow(key, data);
    }

    private String getData() {
        StringBuilder sb = new StringBuilder();

        if (context.getTupleDescription().get(0).isProjected()) {
            sb.append(firstColumn);
        } else {
            // Specifies the string that represents a NULL value.
            // The default is \N (backslash-N) in TEXT mode, and
            // an empty value with no quotations in CSV mode.
            sb.append("\\N");
        }
        sb.append(userDelimiter);

        if (context.getTupleDescription().get(1).isProjected()) {
            sb.append(counter);
        } else {
            // Specifies the string that represents a NULL value.
            // The default is \N (backslash-N) in TEXT mode, and
            // an empty value with no quotations in CSV mode.
            sb.append("\\N");
        }
        sb.append(userDelimiter);

        if (context.getTupleDescription().get(2).isProjected()) {
            sb.append(counter % 2 == 0);
        } else {
            // Specifies the string that represents a NULL value.
            // The default is \N (backslash-N) in TEXT mode, and
            // an empty value with no quotations in CSV mode.
            sb.append("\\N");
        }
        sb.append(userDelimiter);

        if (context.getTupleDescription().get(3).isProjected()) {
            sb.append(userData);
        } else {
            // Specifies the string that represents a NULL value.
            // The default is \N (backslash-N) in TEXT mode, and
            // an empty value with no quotations in CSV mode.
            sb.append("\\N");
        }

        return sb.toString();
    }

    @Override
    public void closeForRead() throws Exception {
    }

    @Override
    public boolean openForWrite() throws Exception {
        throw new UnsupportedOperationException("openForWrite method is not implemented");
    }

    @Override
    public boolean writeNextObject(OneRow onerow) throws Exception {
        throw new UnsupportedOperationException("writeNextObject method is not implemented");
    }

    @Override
    public void closeForWrite() throws Exception {
        throw new UnsupportedOperationException("closeForWrite method is not implemented");
    }
}
