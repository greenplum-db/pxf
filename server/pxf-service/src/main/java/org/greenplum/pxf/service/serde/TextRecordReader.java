package org.greenplum.pxf.service.serde;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.ResultIterator;
import com.univocity.parsers.common.fields.FieldSet;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.common.record.RecordMetaData;
import com.univocity.parsers.conversions.Conversions;
import com.univocity.parsers.conversions.ObjectConversion;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.GreenplumCSV;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;

import java.io.DataInput;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A RecordReader that reads data from an input stream and deserializes database tuples encoded in TEXT format.
 */
public class TextRecordReader extends BaseRecordReader {

    // max number of columns in a Greenplum table
    // see MaxHeapAttributeNumber in https://github.com/greenplum-db/gpdb/blob/main/src/include/access/htup_details.h
    private static final int MAX_COLUMNS = 1600;

    private final GreenplumCSV greenplumCSV;
    private final CsvParser parser;
    private final PgUtilities pgUtilities;
    private ResultIterator<Record, ParsingContext> iterator;
    private int numColumns;
    private int[] columnTypes;
    private Class<?>[] javaTypes;
    private boolean initialized = false;

    /**
     * Creates a new instance and sets up a CSV parser and its settings
     * @param context request context
     * @param pgUtilities an instance of utilities with helper methods for binary and array types
     */
    public TextRecordReader(RequestContext context, PgUtilities pgUtilities) {
        super(context);
        this.pgUtilities = pgUtilities;
        greenplumCSV = context.getGreenplumCSV(); // get the specification of CSV parameters

        // set parser settings based on data from greenplumCSV
        CsvFormat csvFormat = new CsvFormat();
        csvFormat.setDelimiter(greenplumCSV.getDelimiter());
        csvFormat.setLineSeparator(greenplumCSV.getNewline());
        csvFormat.setQuote(greenplumCSV.getQuote());
        csvFormat.setQuoteEscape(greenplumCSV.getEscape());

        // adjust parser setting to be appropriate for our on-the-wire format of CSV / TSV serialization
        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.setFormat(csvFormat);
        parserSettings.setCommentProcessingEnabled(false);  // there should be no comments, do not waste time analyzing
        parserSettings.setIgnoreLeadingWhitespaces(false);  // do not remove any whitespaces
        parserSettings.setIgnoreTrailingWhitespaces(false); // do not remove any whitespaces
        parserSettings.setMaxColumns(MAX_COLUMNS);          // align max columns with Greenplum spec
        // we should've set maxCharsPerColumn value to 1GB (max size in GP) or larger (for multibyte UTF8 chars)
        // but Univocity tries to allocate the buffer of this size ahead of time, which is very inefficient
        // parserSettings.setMaxCharsPerColumn(Integer.MAX_VALUE);

        // create the CSV parser with desired settings
        parser = new CsvParser(parserSettings);

        if (LOG.isDebugEnabled()) {
            // replace new line characters so that the log message takes only 1 line
            LOG.debug("Configured CSV Parser : {}", csvFormat.toString()
                    .replace("\n", " ")
                    .replace("\t", "|"));
        }
    }

    private void initialize() {
        // provide for handling of custom null values, if applicable
        RecordMetaData metadata = parser.getRecordMetadata();
        String nullValue = greenplumCSV.getValueOfNull();
        if (StringUtils.isNotBlank(nullValue)) {
            LOG.debug("Setting custom value of NULL to {}", nullValue);
            FieldSet<Integer> fieldSet = metadata.convertIndexes(Conversions.toNull(nullValue));
            fieldSet.set(IntStream.range(0, columnDescriptors.size()).boxed().collect(Collectors.toList()));
        }

        // setup arrays for quick access to data types and java types by column index
        numColumns = columnDescriptors.size();
        columnTypes = new int[numColumns];
        javaTypes = new Class[numColumns];
        FieldSet<Integer> booleanFields = metadata.convertIndexes(Conversions.toBoolean("t", "f"));
        FieldSet<Integer> binaryFields = metadata.convertIndexes(new BinaryConversion());
        for (int columnIndex = 0; columnIndex < numColumns; columnIndex++) {
            DataType dataType = columnDescriptors.get(columnIndex).getDataType();
            int columnType = dataType.getDeserializationType().getOID();
            columnTypes[columnIndex] = columnType;
            javaTypes[columnIndex] = getJavaClass(dataType);
            // process value conversions
            switch (dataType) {
                case BOOLEAN:
                    booleanFields.add(columnIndex);
                    break;
                case BYTEA:
                    binaryFields.add(columnIndex);
                    break;
            }
        }
        initialized = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<OneField> readRecord(DataInput input) throws Exception {
        if (iterator == null) {
            iterator = parser.iterateRecords((InputStream) input, context.getDatabaseEncoding()).iterator();
        }

        if (!iterator.hasNext()) {
            return null; // no more data to read
        }

        // this can only be done after iterator read a record or called hasNext()
        if (!initialized) {
            initialize();
        }

        // parse a new record from the input stream
        Record csvRecord = iterator.next();

        // make sure the number of fields is the same as the number of columns
        int numFields = csvRecord.getValues().length;
        if (numFields != numColumns) {
            throw new PxfRuntimeException(
                    String.format("Number of record fields %d is not equal to the number of table columns %d",
                            numFields, numColumns));
        }

        // create the target record to be returned
        List<OneField> record = new ArrayList<>(numColumns);

        // convert record to a List of OneField objects according to the column types
        for (int columnIndex = 0; columnIndex < numColumns; columnIndex++) {
            Object fieldValue = csvRecord.getValue(columnIndex, javaTypes[columnIndex]);
            // Univocity cannot handle custom converter with null value, so we have to convert BYTEA here ourselves
            if (columnTypes[columnIndex] == DataType.BYTEA.getOID() && fieldValue != null) {
                fieldValue = pgUtilities.parseByteaLiteral((String) fieldValue);
            }
            record.add(new OneField(columnTypes[columnIndex], fieldValue));
        }

        return record;
    }

    private Class getJavaClass(DataType dataType) {
        // only very specific numeric types will get their own functions
        // all other data types are considered as Strings
        switch (dataType) {
            case BOOLEAN:
                return Boolean.class;
         // this section below should've been here, but since I could not get univocity to return null value properly
         // for the custom BinaryConversion, we treat BYTEA as a String here and will do parsing in readRecord method
         // case BYTEA:
         //     return ByteBuffer.class;
            case BIGINT:
                return Long.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
                return Integer.class;
            case REAL:
                return Float.class;
            case FLOAT8:
                return Double.class;
            default:
                // everything else was serialized as a string and will be further converted by a resolver
                return String.class;
        }
    }

    /**
     * Converts Strings that contain Greenplum binary data in escape format to ByteBuffers
     */
    public class BinaryConversion extends ObjectConversion<ByteBuffer> {

        /**
         * Creates a Conversion from String to ByteBuffer with default values to return when the input is null.
         * This default constructor assumes the output of a conversion should be null when input is null
         */
        public BinaryConversion() {
            super();
        }

        /**
         * Creates a Conversion from String to ByteBuffer with default values to return when the input is null.
         * @param valueIfStringIsNull default ByteBuffer value to be returned when the input String is null. Used when {@link ObjectConversion#execute(String)} is invoked.
         * @param valueIfObjectIsNull default String value to be returned when a ByteBuffer input is null. Used when {@code revert(ByteBuffer)} is invoked.
         */
        public BinaryConversion(ByteBuffer valueIfStringIsNull, String valueIfObjectIsNull) {
            super(valueIfStringIsNull, valueIfObjectIsNull);
        }

        /**
         * Converts a String to Byte.
         */
        @Override
        protected ByteBuffer fromString(String input) {
            return input == null ? null : pgUtilities.parseByteaLiteral(input);
        }
    }
}
