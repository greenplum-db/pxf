package org.greenplum.pxf.automation.testplugin;

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.ReadAccessor;
import org.greenplum.pxf.api.utilities.InputData;
import org.greenplum.pxf.api.utilities.Plugin;

/**
 * Test class for regression tests.
 * The only thing this class does is to take received user data and
 * return it as the third column value on each readNextObject call.
 * The returned data has 4 columns delimited with DELIMITER property value.
 * First column - text, second column - int (counter), third column - bool, fourth column - text.
 */
public class UserDataVerifyAccessor extends Plugin implements ReadAccessor
{
    private String userData;
    private String userDelimiter;

    private int counter = 0;
    private char firstColumn = 'A';

    public UserDataVerifyAccessor(InputData input) {
        super(input);
    }

    @Override
    public boolean openForRead() throws Exception {

        userData = new String(inputData.getFragmentUserData());
        userDelimiter = inputData.getUserProperty("DELIMITER");

        return true;
    }

    @Override
    public OneRow readNextObject() {

        // Termination rule
        if (counter >= 10) {
            return null;
        }

        // Generate tuple with user data value as last column.
        String data = firstColumn + userDelimiter +  new Integer(counter).toString() + userDelimiter +  (counter % 2 == 0) + userDelimiter +  userData;
        String key = new Integer(counter).toString();

        counter++;
        firstColumn++;

        return new OneRow(key, data);
    }

    @Override
    public void closeForRead() throws Exception {
    }
}