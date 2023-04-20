package org.greenplum.pxf.service.serde;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.io.GPDBWritable;
import org.greenplum.pxf.api.model.RequestContext;

import java.io.DataInput;
import java.util.ArrayList;
import java.util.List;

/**
 * Record reader that reads data from an input stream and deserializes database tuples encoded in GPDBWritable format.
 */
public class GPDBWritableRecordReader extends BaseRecordReader implements RecordReader {

    /**
     * Creates a new instance
     * @param context request context
     */
    public GPDBWritableRecordReader(RequestContext context) {
        super(context);
    }

    @Override
    public List<OneField> readRecord(DataInput input) throws Exception {
        GPDBWritable gpdbWritable = new GPDBWritable(databaseEncoding);
        gpdbWritable.readFields(input);

        if (gpdbWritable.isEmpty()) {
            LOG.debug("Reached end of stream");
            return null;
        }

        // TODO: can mapper be initialized once the first time on initially (based on columnDescriptors) ?
        GPDBWritableMapper mapper = new GPDBWritableMapper(gpdbWritable);
        int[] colTypes = gpdbWritable.getColType();
        List<OneField> record = new ArrayList<>(colTypes.length);
        for (int i = 0; i < colTypes.length; i++) {
            mapper.setDataType(colTypes[i]);
            record.add(new OneField(colTypes[i], mapper.getData(i)));
        }
        return record;
    }
}
