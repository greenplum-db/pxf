package org.greenplum.pxf.plugins.hdfs.orc;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.greenplum.pxf.api.GreenplumDateTime;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.plugins.hdfs.utilities.PgArrayBuilder;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.function.Function;

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
    private static PgUtilities pgUtilities = new PgUtilities();

    public static OneField[] booleanMapper(VectorizedRowBatch batch, ColumnVector columnVector, int oid) {
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
     *
     * @param batch        the column batch to be processed
     * @param columnVector the ListColumnVector that contains another ColumnVector containing the actual data
     * @param oid          the destination GPDB column OID
     * @return returns an array of OneFields, where each element in the array contains data from an entire row as a String
     */
    public static OneField[] listMapper(VectorizedRowBatch batch, ColumnVector columnVector, int oid) {
        ListColumnVector listColumnVector = (ListColumnVector) columnVector;
        if (listColumnVector == null) {
            return getNullResultSet(oid, batch.size);
        }

        OneField[] result = new OneField[batch.size];
        // if the row is repeated, then we only need to serialize the row once.
        String repeatedRow = listColumnVector.isRepeating ? serializeListRow(listColumnVector, 0, oid) : null;
        for (int rowIndex = 0; rowIndex < batch.size; rowIndex++) {
            String value = listColumnVector.isRepeating ? repeatedRow : serializeListRow(listColumnVector, rowIndex, oid);
            result[rowIndex] = new OneField(oid, value);
        }

        return result;
    }

    /**
     * Helper method for handling the underlying data of ORC list compound types
     *
     * @param columnVector the ListColumnVector containing the array data
     * @param row          the row of data to pull out from the ColumnVector
     * @param oid          the GPDB mapping of the ORC list compound type
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
     *
     * @param bytesColumnVector the ColumnVector containing the byte array data
     * @param row               the index of a row of data to pull out from the ColumnVector
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

    public static OneField[] shortMapper(VectorizedRowBatch batch, ColumnVector columnVector, int oid) {
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

    public static OneField[] integerMapper(VectorizedRowBatch batch, ColumnVector columnVector, int oid) {
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

    public static OneField[] longMapper(VectorizedRowBatch batch, ColumnVector columnVector, int oid) {
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

    public static OneField[] floatMapper(VectorizedRowBatch batch, ColumnVector columnVector, int oid) {
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

    public static OneField[] doubleMapper(VectorizedRowBatch batch, ColumnVector columnVector, int oid) {
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

    public static OneField[] textMapper(VectorizedRowBatch batch, ColumnVector columnVector, Integer oid) {
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

    public static OneField[] decimalMapper(VectorizedRowBatch batch, ColumnVector columnVector, Integer oid) {
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

    public static OneField[] binaryMapper(VectorizedRowBatch batch, ColumnVector columnVector, Integer oid) {
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
    public static OneField[] dateMapper(VectorizedRowBatch batch, ColumnVector columnVector, Integer oid) {
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

    public static OneField[] timestampMapper(VectorizedRowBatch batch, ColumnVector columnVector, Integer oid) {
        return timestampMapperWithFormatter(batch, columnVector, oid, ORCVectorizedMappingFunctions::timestampToString);
    }

    public static OneField[] timestampWithTimeZoneMapper(VectorizedRowBatch batch, ColumnVector columnVector, Integer oid) {
        return timestampMapperWithFormatter(batch, columnVector, oid, ORCVectorizedMappingFunctions::timestampWithTimeZoneToString);
    }

    public static OneField[] timestampMapperWithFormatter(VectorizedRowBatch batch, ColumnVector columnVector, Integer oid, Function<Timestamp, String> formatter) {
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
                    ? formatter.apply(tcv.asScratchTimestamp(rowId))
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

    /**
     * Converts Timestamp objects to the String representation given the
     * Greenplum DATETIME_FORMATTER
     *
     * @param timestamp the timestamp object
     * @return the string representation of the timestamp
     */
    private static String timestampToString(Timestamp timestamp) {
        return timestampToStringWithFormatter(timestamp, GreenplumDateTime.DATETIME_FORMATTER);
    }

    private static String timestampWithTimeZoneToString(Timestamp timestamp) {
        return timestampToStringWithFormatter(timestamp, GreenplumDateTime.DATETIME_WITH_TIMEZONE_FORMATTER);
    }

    private static String timestampToStringWithFormatter(Timestamp timestamp, DateTimeFormatter formatter) {
        Instant instant = timestamp.toInstant();
        String timestampString = instant
                .atZone(ZoneId.systemDefault())
                .format(formatter);
        LOG.debug("Converted timestamp: {} to date: {}", timestamp, timestampString);
        return timestampString;
    }

}
