package org.greenplum.pxf.service.serde;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.ResultIterator;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.GreenplumCSV;
import org.greenplum.pxf.api.model.RequestContext;

import java.io.DataInput;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Record reader that reads data from an input stream and deserializes database tuples encoded in TEXT format.
 */
public class TextRecordReader extends BaseRecordReader implements RecordReader {

    private final GreenplumCSV greenplumCSV;
    private final CsvParser parser;
    private ResultIterator<Record, ParsingContext> iterator;

    /**
     * Creates a new instance
     * @param context request context
     */
    public TextRecordReader(RequestContext context) {
        super(context);
        this.greenplumCSV = context.getGreenplumCSV(); // get the specification of CSV parameters
        // TODO: set CsvParserSettings from grennplumCSV
        parser = new CsvParser(new CsvParserSettings());
        //List<String[]> parsedRows = parser.parseAll(inputReader);
    }

    @Override
    public List<OneField> readRecord(DataInput input) throws Exception {
        Charset databaseEncoding = context.getDatabaseEncoding();
        if (iterator == null) {
            iterator = parser.iterateRecords((InputStream) input, databaseEncoding).iterator();
        }

        // parse a new record from the input stream
        Record csvRecord = iterator.next();
        if (csvRecord == null) {
            return null; // no more data to read
        }

        // make sure the number of fields is the same as the number of columns
        int numFields = csvRecord.getValues().length;
        int numColumns = columnDescriptors.size();
        if (numFields != numColumns) {
            throw new PxfRuntimeException(
                    String.format("Number of fields received %d is not equal to the number of table columns %d",
                            numFields, numColumns));
        }

        // create the target record to be returned
        List<OneField> record = new ArrayList<>(numColumns);

        // convert record to a List of OneField objects according to the column types
        for (int columnIndex = 0; columnIndex < numColumns; columnIndex++) {
            // TODO: conversions
        }

        return record;
    }
}
