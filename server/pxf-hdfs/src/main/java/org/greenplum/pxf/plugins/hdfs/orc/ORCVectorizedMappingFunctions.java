package org.greenplum.pxf.plugins.hdfs.orc;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.greenplum.pxf.api.GreenplumDateTime;
import org.greenplum.pxf.api.OneField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;

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
