package org.greenplum.pxf.automation.testplugin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;

/*
 * Internal interface that defines the access to a file on HDFS.  All classes
 * that implement actual access to an HDFS file (sequence file, avro file,...)
 * must respect this interface
 * Dummy implementation, for documentation
 */
public class DummyAccessor extends BasePlugin implements Accessor {
    private static final Log LOG = LogFactory.getLog(DummyAccessor.class);
    private int rowNumber;
    private int fragmentNumber;

    @Override
    public boolean openForRead() throws Exception {
        /* fopen or similar */
        return true;
    }

    @Override
    public OneRow readNextObject() throws Exception {
        /* return next row , <key=fragmentNo.rowNo, val=rowNo,text,fragmentNo>*/
        /* check for EOF */
        if (fragmentNumber > 0) {
            return null; /* signal EOF, close will be called */
        }

        int fragment = context.getDataFragment();
        String fragmentMetadata = new String(context.getFragmentMetadata());
        /* generate row */
        OneRow row = new OneRow(fragment + "." + rowNumber, /* key */
                rowNumber + "," + fragmentMetadata + "," + fragment /* value */);
        /* advance */
        rowNumber += 1;
        if (rowNumber == 2) {
            rowNumber = 0;
            fragmentNumber += 1;
        }
        /* return data */
        return row;
    }

    @Override
    public void closeForRead() throws Exception {
        /* fclose or similar */
    }

    @Override
    public boolean openForWrite() throws Exception {
        /* fopen or similar */
        return true;
    }

    @Override
    public boolean writeNextObject(OneRow onerow) throws Exception {

        LOG.info(onerow.getData());
        return true;
    }

    @Override
    public void closeForWrite() throws Exception {
        /* fclose or similar */
    }
}
