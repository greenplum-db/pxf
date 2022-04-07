package org.greenplum.pxf.plugins.hdfs.orc;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.TypeDescription;
import org.greenplum.pxf.api.GreenplumDateTime;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.function.TriConsumer;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.plugins.hdfs.utilities.PgArrayBuilder;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

/**
 * Maps vectors of ORC types to a list of OneFields.
 * ORC provides a rich set of scalar and compound types. The mapping is as
 * follows
 * <p>
 * ---------------------------------------------------------------------------
 * | ORC Physical Type | ORC Logical Type   | Greenplum Type | Greenplum OID |
 * ---------------------------------------------------------------------------
 * |  Integer          |  boolean  (1 bit)  |  BOOLEAN       |  16           |
 * |  Integer          |  tinyint  (8 bit)  |  SMALLINT      |  21           |
 * |  Integer          |  smallint (16 bit) |  SMALLINT      |  21           |
 * |  Integer          |  int      (32 bit) |  INTEGER       |  23           |
 * |  Integer          |  bigint   (64 bit) |  BIGINT        |  20           |
 * |  Integer          |  date              |  DATE          |  1082         |
 * |  Floating point   |  float             |  REAL          |  700          |
 * |  Floating point   |  double            |  FLOAT8        |  701          |
 * |  String           |  string            |  TEXT          |  25           |
 * |  String           |  char              |  BPCHAR        |  1042         |
 * |  String           |  varchar           |  VARCHAR       |  1043         |
 * |  Byte[]           |  binary            |  BYTEA         |  17           |
 * ---------------------------------------------------------------------------
 */
class ORCVectorizedMappingFunctions {

    private static final Logger LOG = LoggerFactory.getLogger(ORCVectorizedMappingFunctions.class);

    // we intentionally create a new instance of PgUtilities here due to unnecessary complexity
    // required for dependency injection
    private static final PgUtilities pgUtilities = new PgUtilities();

    private static final Map<TypeDescription.Category, TriConsumer<ColumnVector, Integer, Object>> writeFunctionsMap;
    static {
        writeFunctionsMap = new EnumMap<>(TypeDescription.Category.class);
        initWriteFunctionsMap();
    }

    public static OneField[] booleanReader(VectorizedRowBatch batch, ColumnVector columnVector, int oid) {
        LongColumnVector lcv = (LongColumnVector) columnVector;
        if (lcv == null)
            return getNullResultSet(oid, batch.size);

        OneField[] result = new OneField[batch.size];
        int m = lcv.isRepeating ? 0 : 1;
        int rowId;
        Boolean value;
        for (int rowIndex = 0; rowIndex < batch.size; rowIndex++) {
            rowId = m * rowIndex;
            value = (lcv.noNulls || !lcv.isNull[rowId])
                    ? lcv.vector[rowId] == 1
                    : null;
            result[rowIndex] = new OneField(oid, value);
        }
        return result;
    }

    /**
     * Serializes ORC lists of PXF-supported primitives into Postgres array syntax
     * @param batch the column batch to be processed
     * @param columnVector the ListColumnVector that contains another ColumnVector containing the actual data
     * @param oid the destination GPDB column OID
     * @return returns an array of OneFields, where each element in the array contains data from an entire row as a String
     */
    public static OneField[] listReader(VectorizedRowBatch batch, ColumnVector columnVector, int oid) {
        ListColumnVector listColumnVector = (ListColumnVector) columnVector;
        if (listColumnVector == null) {
            return getNullResultSet(oid, batch.size);
        }

        OneField[] result = new OneField[batch.size];
        // if the row is repeated, then we only need to serialize the row once.
        String repeatedRow = listColumnVector.isRepeating ? serializeListRow(listColumnVector,0, oid) : null;
        for (int rowIndex = 0; rowIndex < batch.size; rowIndex++) {
            String value = listColumnVector.isRepeating ? repeatedRow : serializeListRow(listColumnVector, rowIndex, oid);
            result[rowIndex] = new OneField(oid, value);
        }

        return result;
    }

    /**
     * Helper method for handling the underlying data of ORC list compound types
     * @param columnVector the ListColumnVector containing the array data
     * @param row the row of data to pull out from the ColumnVector
     * @param oid the GPDB mapping of the ORC list compound type
     * @return the data of the given row as a string
     */
    public static String serializeListRow(ListColumnVector columnVector, int row, int oid) {
        if (columnVector.isNull[row]) {
            return null;
        }

        int length = (int) columnVector.lengths[row];
        int offset = (int) columnVector.offsets[row];

        PgArrayBuilder pgArrayBuilder = new PgArrayBuilder(pgUtilities);
        pgArrayBuilder.startArray();
        for (int i = 0; i < length; i++) {
            int childRow = offset + i;

            switch (columnVector.child.type) {
                case LIST:
                    pgArrayBuilder.addElementNoEscaping(serializeListRow((ListColumnVector) columnVector.child, childRow, oid));
                    break;
                case BYTES:
                    // if the type of the column vector is BYTES, then the underlying datatype could be any
                    // ORC string type. This could map to GPDB bytea array, text array or even varchar/bpchar.
                    if (oid == DataType.BYTEAARRAY.getOID()) {
                        // bytea arrays require special handling due to the encoding type. Handle that here.
                        ByteBuffer byteBuffer = getByteBuffer((BytesColumnVector) columnVector.child, childRow);
                        pgArrayBuilder.addElementNoEscaping(byteBuffer == null ? "NULL" : pgUtilities.encodeAndEscapeByteaHex(byteBuffer));
                    } else {
                        // the type here could be something like TEXT, BPCHAR, VARCHAR
                        pgArrayBuilder.addElement(((BytesColumnVector) columnVector.child).toString(childRow));
                    }
                    break;
                default:
                    pgArrayBuilder.addElement(buf -> columnVector.child.stringifyValue(buf, childRow));
            }
        }
        pgArrayBuilder.endArray();
        return pgArrayBuilder.toString();
    }

    /**
     * Wraps a byte array for a given row index into a byte buffer
     * @param bytesColumnVector the ColumnVector containing the byte array data
     * @param row the index of a row of data to pull out from the ColumnVector
     * @return the ByteBuffer for the given row
     */
    private static ByteBuffer getByteBuffer(BytesColumnVector bytesColumnVector, int row) {
        if (bytesColumnVector.isRepeating) {
            row = 0;
        }
        if (bytesColumnVector.noNulls || !bytesColumnVector.isNull[row]) {
            return ByteBuffer.wrap(bytesColumnVector.vector[row], bytesColumnVector.start[row], bytesColumnVector.length[row]);
        } else {
            return null;
        }
    }

    public static OneField[] shortReader(VectorizedRowBatch batch, ColumnVector columnVector, int oid) {
        LongColumnVector lcv = (LongColumnVector) columnVector;
        if (lcv == null)
            return getNullResultSet(oid, batch.size);

        OneField[] result = new OneField[batch.size];
        int m = lcv.isRepeating ? 0 : 1;
        int rowId;
        Short value;
        for (int rowIndex = 0; rowIndex < batch.size; rowIndex++) {
            rowId = m * rowIndex;
            value = (lcv.noNulls || !lcv.isNull[rowId])
                    ? (short) lcv.vector[rowId]
                    : null;
            result[rowIndex] = new OneField(oid, value);
        }
        return result;
    }

    public static OneField[] integerReader(VectorizedRowBatch batch, ColumnVector columnVector, int oid) {
        LongColumnVector lcv = (LongColumnVector) columnVector;
        if (lcv == null)
            return getNullResultSet(oid, batch.size);

        OneField[] result = new OneField[batch.size];
        int m = lcv.isRepeating ? 0 : 1;
        int rowId;
        Integer value;
        for (int rowIndex = 0; rowIndex < batch.size; rowIndex++) {
            rowId = m * rowIndex;
            value = (lcv.noNulls || !lcv.isNull[rowId])
                    ? (int) lcv.vector[rowId]
                    : null;
            result[rowIndex] = new OneField(oid, value);
        }
        return result;
    }

    public static OneField[] longReader(VectorizedRowBatch batch, ColumnVector columnVector, int oid) {
        LongColumnVector lcv = (LongColumnVector) columnVector;
        if (lcv == null)
            return getNullResultSet(oid, batch.size);

        OneField[] result = new OneField[batch.size];
        int m = lcv.isRepeating ? 0 : 1;
        int rowId;
        Long value;
        for (int rowIndex = 0; rowIndex < batch.size; rowIndex++) {
            rowId = m * rowIndex;
            value = (lcv.noNulls || !lcv.isNull[rowId])
                    ? lcv.vector[rowId]
                    : null;
            result[rowIndex] = new OneField(oid, value);
        }
        return result;
    }

    public static OneField[] floatReader(VectorizedRowBatch batch, ColumnVector columnVector, int oid) {
        DoubleColumnVector dcv = (DoubleColumnVector) columnVector;
        if (dcv == null)
            return getNullResultSet(oid, batch.size);

        OneField[] result = new OneField[batch.size];
        int m = dcv.isRepeating ? 0 : 1;
        int rowId;
        Float value;
        for (int rowIndex = 0; rowIndex < batch.size; rowIndex++) {
            rowId = m * rowIndex;
            value = (dcv.noNulls || !dcv.isNull[rowId])
                    ? (float) dcv.vector[rowId]
                    : null;
            result[rowIndex] = new OneField(oid, value);
        }
        return result;
    }

    public static OneField[] doubleReader(VectorizedRowBatch batch, ColumnVector columnVector, int oid) {
        DoubleColumnVector dcv = (DoubleColumnVector) columnVector;
        if (dcv == null)
            return getNullResultSet(oid, batch.size);

        OneField[] result = new OneField[batch.size];
        int m = dcv.isRepeating ? 0 : 1;
        int rowId;
        Double value;
        for (int rowIndex = 0; rowIndex < batch.size; rowIndex++) {
            rowId = m * rowIndex;
            value = (dcv.noNulls || !dcv.isNull[rowId])
                    ? dcv.vector[rowId]
                    : null;
            result[rowIndex] = new OneField(oid, value);
        }
        return result;
    }

    public static OneField[] textReader(VectorizedRowBatch batch, ColumnVector columnVector, Integer oid) {
        BytesColumnVector bcv = (BytesColumnVector) columnVector;
        if (bcv == null)
            return getNullResultSet(oid, batch.size);

        OneField[] result = new OneField[batch.size];
        int m = bcv.isRepeating ? 0 : 1;
        int rowId;
        String value;
        for (int rowIndex = 0; rowIndex < batch.size; rowIndex++) {
            rowId = m * rowIndex;

            value = bcv.noNulls || !bcv.isNull[rowId] ?
                    new String(bcv.vector[rowId], bcv.start[rowId],
                            bcv.length[rowId], StandardCharsets.UTF_8) : null;
            result[rowIndex] = new OneField(oid, value);
        }
        return result;
    }

    public static OneField[] decimalReader(VectorizedRowBatch batch, ColumnVector columnVector, Integer oid) {
        DecimalColumnVector dcv = (DecimalColumnVector) columnVector;
        if (dcv == null)
            return getNullResultSet(oid, batch.size);

        OneField[] result = new OneField[batch.size];
        int m = dcv.isRepeating ? 0 : 1;
        int rowId;
        HiveDecimalWritable value;
        for (int rowIndex = 0; rowIndex < batch.size; rowIndex++) {
            rowId = m * rowIndex;
            value = (dcv.noNulls || !dcv.isNull[rowId])
                    ? dcv.vector[rowId]
                    : null;
            result[rowIndex] = new OneField(oid, value);
        }
        return result;
    }

    public static OneField[] binaryReader(VectorizedRowBatch batch, ColumnVector columnVector, Integer oid) {
        BytesColumnVector bcv = (BytesColumnVector) columnVector;
        if (bcv == null)
            return getNullResultSet(oid, batch.size);

        OneField[] result = new OneField[batch.size];
        int m = bcv.isRepeating ? 0 : 1;
        int rowId;
        byte[] value;
        for (int rowIndex = 0; rowIndex < batch.size; rowIndex++) {
            rowId = m * rowIndex;
            if (bcv.noNulls || !bcv.isNull[rowId]) {
                value = new byte[bcv.length[rowId]];
                System.arraycopy(bcv.vector[rowId], bcv.start[rowId], value, 0, bcv.length[rowId]);
            } else {
                value = null;
            }
            result[rowIndex] = new OneField(oid, value);
        }
        return result;
    }

    // DateWritable is no longer deprecated in newer versions of storage api ¯\_(ツ)_/¯
    @SuppressWarnings("deprecation")
    public static OneField[] dateReader(VectorizedRowBatch batch, ColumnVector columnVector, Integer oid) {
        LongColumnVector lcv = (LongColumnVector) columnVector;
        if (lcv == null)
            return getNullResultSet(oid, batch.size);

        OneField[] result = new OneField[batch.size];
        int m = lcv.isRepeating ? 0 : 1;
        int rowId;
        Date value;
        for (int rowIndex = 0; rowIndex < batch.size; rowIndex++) {
            rowId = m * rowIndex;
            value = (lcv.noNulls || !lcv.isNull[rowId])
                    ? Date.valueOf(LocalDate.ofEpochDay(lcv.vector[rowId]))
                    : null;
            result[rowIndex] = new OneField(oid, value);
        }
        return result;
    }

    public static OneField[] timestampReader(VectorizedRowBatch batch, ColumnVector columnVector, Integer oid) {
        TimestampColumnVector tcv = (TimestampColumnVector) columnVector;
        if (tcv == null)
            return getNullResultSet(oid, batch.size);

        OneField[] result = new OneField[batch.size];
        int m = tcv.isRepeating ? 0 : 1;
        int rowId;
        String value;
        for (int rowIndex = 0; rowIndex < batch.size; rowIndex++) {
            rowId = m * rowIndex;
            value = (tcv.noNulls || !tcv.isNull[rowId])
                    ? timestampToString(tcv.asScratchTimestamp(rowId))
                    : null;
            result[rowIndex] = new OneField(oid, value);
        }
        return result;
    }

    public static OneField[] getNullResultSet(int oid, int size) {
        OneField[] result = new OneField[size];
        Arrays.fill(result, new OneField(oid, null));
        return result;
    }

    public static TriConsumer<ColumnVector, Integer, Object> getColumnWriter(TypeDescription typeDescription) {
        TypeDescription.Category columnTypeCategory = typeDescription.getCategory();
        TriConsumer<ColumnVector, Integer, Object> writeFunction = writeFunctionsMap.get(columnTypeCategory);
        if (writeFunction == null) {
            throw new PxfRuntimeException("Unsupported ORC type " + columnTypeCategory);
        }
        return writeFunction;
    }

    /**
     * Initializes functions that update column vector of specific types, registers them into an enum map
     * keyed of from the type description category for lookup by consumers later.
     */
    private static void initWriteFunctionsMap() {
        // TODO: what about nulls ? Can we set null value or do we need to update flags on VectorizedRowBatch ?
        // TODO: the logic below assumes values are not nulls and does not do any null checking
        // TODO: what will we do with repeating ?

        // see TypeUtils.createColumn for backing storage of different Category types
        // go in the order TypeDescription.Category enum is defined
        writeFunctionsMap.put(TypeDescription.Category.BOOLEAN, (columnVector, row, val) -> {
            ((LongColumnVector) columnVector).vector[row] = (Boolean) val ? 1 : 0;
        });
        // BYTE("tinyint", true) - for now ORCSchemaBuilder does not support this type, so we do not expect it
        writeFunctionsMap.put(TypeDescription.Category.SHORT, (columnVector, row, val) -> {
            ((LongColumnVector) columnVector).vector[row] = ((Number) val).longValue();
        });
        writeFunctionsMap.put(TypeDescription.Category.INT, (columnVector, row, val) -> {
            ((LongColumnVector) columnVector).vector[row] = ((Number) val).longValue();
        });
        writeFunctionsMap.put(TypeDescription.Category.LONG, (columnVector, row, val) -> {
            ((LongColumnVector) columnVector).vector[row] = ((Number) val).longValue();
        });
        writeFunctionsMap.put(TypeDescription.Category.FLOAT, (columnVector, row, val) -> {
            ((DoubleColumnVector) columnVector).vector[row] = ((Number) val).floatValue();
        });
        writeFunctionsMap.put(TypeDescription.Category.DOUBLE, (columnVector, row, val) -> {
            ((DoubleColumnVector) columnVector).vector[row] = ((Number) val).doubleValue();
        });
        writeFunctionsMap.put(TypeDescription.Category.STRING, (columnVector, row, val) -> {
            byte[] buffer = val.toString().getBytes(StandardCharsets.UTF_8);
            ((BytesColumnVector) columnVector).setRef(row, buffer, 0, buffer.length);
        });
        writeFunctionsMap.put(TypeDescription.Category.DATE, (columnVector, row, val) -> {
            // parse Greenplum date given as a string to a local date (no timezone info)
            LocalDate date = LocalDate.parse((String) val, GreenplumDateTime.DATE_FORMATTER);
            // convert local date to days since epoch and store in DateColumnVector
            ((DateColumnVector) columnVector).vector[row] = date.toEpochDay();
        });
        writeFunctionsMap.put(TypeDescription.Category.TIMESTAMP, (columnVector, row, val) -> {
            // parse Greenplum timestamp given as a string to a local dateTime (no timezone info)
            LocalDateTime dateTime = LocalDateTime.parse((String) val, GreenplumDateTime.DATETIME_FORMATTER);
            // convert local dateTime to a Timestamp and store in TimestampColumnVector
            ((TimestampColumnVector) columnVector).set(row, Timestamp.valueOf(dateTime));
        });
        writeFunctionsMap.put(TypeDescription.Category.BINARY, (columnVector, row, val) -> {
            // do not copy the contents of the byte array, just set as a reference
            ((BytesColumnVector) columnVector).setRef(row, (byte[]) val, 0, ((byte[]) val).length);
        });
        writeFunctionsMap.put(TypeDescription.Category.DECIMAL, (columnVector, row, val) -> {
            // TODO: review, this implementation makes some assumptions (null check, etc)
            // also there is Decimal and Decimal64 column vectors, see TypeUtils.createColumn
            ((DecimalColumnVector) columnVector).vector[row].set(HiveDecimal.create((BigDecimal) val));
        });

        writeFunctionsMap.put(TypeDescription.Category.VARCHAR, writeFunctionsMap.get(TypeDescription.Category.STRING));

        //TODO: do we need to right-trim CHAR values like we do in Parquet ?
        writeFunctionsMap.put(TypeDescription.Category.CHAR,  writeFunctionsMap.get(TypeDescription.Category.STRING));

        // TODO: LIST collection types
        // MAP("map", false) - for now ORCSchemaBuilder does not support this type, so we do not expect it
        // STRUCT("struct", false) - for now ORCSchemaBuilder does not support this type, so we do not expect it
        // UNION("uniontype", false) - for now ORCSchemaBuilder does not support this type, so we do not expect it

        writeFunctionsMap.put(TypeDescription.Category.TIMESTAMP_INSTANT, (columnVector, row, val) -> {
            // parse Greenplum timestamp given as a string with timezone to a zoned dateTime
            ZonedDateTime zonedDateTime = ZonedDateTime.parse((String) val, GreenplumDateTime.DATETIME_WITH_TIMEZONE_FORMATTER);
            // convert zoned dateTime to a Timestamp and store in TimestampColumnVector
            ((TimestampColumnVector) columnVector).set(row, Timestamp.from(zonedDateTime.toInstant()));
        });
    }

    /**
     * Converts Timestamp objects to the String representation given the
     * Greenplum DATETIME_FORMATTER
     *
     * @param timestamp the timestamp object
     * @return the string representation of the timestamp
     */
    private static String timestampToString(Timestamp timestamp) {
        Instant instant = timestamp.toInstant();
        String timestampString = instant
                .atZone(ZoneId.systemDefault())
                .format(GreenplumDateTime.DATETIME_FORMATTER);
        LOG.debug("Converted timestamp: {} to date: {}", timestamp, timestampString);
        return timestampString;
    }
}
