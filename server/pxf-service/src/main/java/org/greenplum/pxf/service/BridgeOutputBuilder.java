package org.greenplum.pxf.service;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.GreenplumDateTime;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.error.BadRecordException;
import org.greenplum.pxf.api.io.BufferWritable;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.io.GPDBWritable;
import org.greenplum.pxf.api.io.Text;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.GreenplumCSV;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.greenplum.pxf.api.io.DataType.TEXT;

/**
 * Class creates the output record that is piped by the java process to the GPDB
 * backend. Actually, the output record is serialized and the obtained byte
 * string is piped to the GPDB segment. The output record will implement
 * Writable, and the mission of BridgeOutputBuilder will be to translate a list
 * of {@link OneField} objects (obtained from the Resolver) into an output
 * record.
 */
public class BridgeOutputBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(BridgeOutputBuilder.class);

    private static final byte DELIM = 10; /* (byte)'\n'; */
    public static final String PXF_ERROR_TOKEN = "PXFERRMSG> ";
    private final Charset databaseEncoding;
    private final String newLine;
    private final byte[] newLineBytes;
    private Writable output = null;
    private final LinkedList<Writable> outputList;
    private Writable partialLine = null;
    private GPDBWritable errorRecord = null;
    private int[] schema;
    private String[] colNames;
    private final boolean samplingEnabled;
    private final GreenplumCSV greenplumCSV;
    private final OutputFormat outputFormat;
    private final List<ColumnDescriptor> columnDescriptors;
    private final String gpdbTableformat;

    /**
     * Constructs a BridgeOutputBuilder.
     *
     * @param context input data, like requested output format and schema
     *                information
     */
    public BridgeOutputBuilder(RequestContext context) {
        columnDescriptors = context.getTupleDescription();
        greenplumCSV = context.getGreenplumCSV();
        newLine = context.getGreenplumCSV().getNewline();
        newLineBytes = newLine.getBytes(StandardCharsets.UTF_8);
        outputFormat = context.getOutputFormat();
        databaseEncoding = context.getDatabaseEncoding();
        outputList = new LinkedList<>();
        makeErrorRecord();
        samplingEnabled = (context.getStatsSampleRatio() > 0);
        gpdbTableformat = context.getFormat();
    }

    /**
     * We need a separate GPDBWritable record to represent the error record.
     * Just setting the errorFlag on the "output" GPDBWritable variable is not
     * good enough, since the GPDBWritable is built only after the first record
     * is read from the file. And if we encounter an error while fetching the
     * first record from the file, then the output member will be null. The
     * reason we cannot count on the schema to build the GPDBWritable output
     * variable before reading the first record, is because the schema does not
     * account for arrays - we cannot know from the schema the length of an
     * array. We find out only after fetching the first record.
     */
    void makeErrorRecord() {
        int[] errSchema = {TEXT.getOID()};

        if (outputFormat != OutputFormat.GPDBWritable) {
            return;
        }

        errorRecord = new GPDBWritable(errSchema, databaseEncoding);
        errorRecord.setError(true);
    }

    /**
     * Returns the error record. If the output format is not binary, a
     * comma-delimited row will be generated.
     *
     * @param ex exception to be stored in record
     * @return error record
     */
    public Writable getErrorOutput(Exception ex) throws Exception {
        if (outputFormat == OutputFormat.GPDBWritable) {
            errorRecord.setString(0, ex.getMessage());
            return errorRecord;
        } else {
            // Serialize error text into CSV
            // We create a row with an extra column containing the error token (used by FDW) and error information
            LOG.error(ex.getMessage(), ex);
            return new Text(
                    StringUtils.repeat(String.valueOf(greenplumCSV.getDelimiter()), columnDescriptors.size()) +
                            greenplumCSV.toCsvField(PXF_ERROR_TOKEN + ex.getMessage(), true, true, true) +
                            newLine);
        }
    }

    /**
     * Translates recFields (obtained from the Resolver) into an output record.
     *
     * @param recFields record fields to be serialized
     * @return list of Writable objects with serialized row
     * @throws BadRecordException if building the output record failed
     */
    public LinkedList<Writable> makeOutput(List<OneField> recFields)
            throws BadRecordException {
        if (output == null && outputFormat == OutputFormat.GPDBWritable) {
            makeGPDBWritableOutput();
        }

        outputList.clear();

        fillOutputRecord(recFields);

        return outputList;
    }

    public LinkedList<Writable> makeVectorizedOutput(List<List<OneField>> recordsBatch) throws BadRecordException {
        outputList.clear();
        if (recordsBatch != null) {
            for (List<OneField> record : recordsBatch) {
                if (outputFormat == OutputFormat.GPDBWritable) {
                    makeGPDBWritableOutput();
                }
                fillOutputRecord(record);
            }
        }
        return outputList;
    }

    /**
     * Returns whether or not this is a partial line.
     *
     * @return true for a partial line
     */
    public Writable getPartialLine() {
        return partialLine;
    }

    /**
     * Creates the GPDBWritable object. The object is created one time and is
     * refilled from recFields for each record sent
     *
     * @return empty GPDBWritable object with set columns
     */
    GPDBWritable makeGPDBWritableOutput() {
        int num_actual_fields = columnDescriptors.size();
        schema = new int[num_actual_fields];
        colNames = new String[num_actual_fields];

        for (int i = 0; i < num_actual_fields; i++) {
            schema[i] = columnDescriptors.get(i).columnTypeCode();
            colNames[i] = columnDescriptors.get(i).columnName();
        }

        output = new GPDBWritable(schema, databaseEncoding);

        return (GPDBWritable) output;
    }

    /**
     * Fills the output record based on the fields in recFields.
     *
     * @param recFields record fields
     * @throws BadRecordException if building the output record failed
     */
    void fillOutputRecord(List<OneField> recFields) throws BadRecordException {
        if (outputFormat == OutputFormat.GPDBWritable) {
            fillGPDBWritable(recFields);
        } else {
            fillText(recFields);
        }
    }

    /**
     * Fills a GPDBWritable object based on recFields. The input record
     * recFields must correspond to schema. If the record has more or less
     * fields than the schema we throw an exception. We require that the type of
     * field[i] in recFields corresponds to the type of field[i] in the schema.
     *
     * @param recFields record fields
     * @throws BadRecordException if building the output record failed
     */
    void fillGPDBWritable(List<OneField> recFields) throws BadRecordException {
        int size = recFields.size();
        if (size == 0) { // size 0 means the resolver couldn't deserialize any
            // of the record fields
            throw new BadRecordException("No fields in record");
        } else if (size != schema.length) {
            throw new BadRecordException("Record has " + size
                    + " fields but the schema size is " + schema.length);
        }

        for (int i = 0; i < size; i++) {
            OneField current = recFields.get(i);
            if (!isTypeInSchema(current.type, schema[i])) {
                throw new BadRecordException(
                        String.format("For field %s schema requires type %s but input record has type %s",
                                colNames[i],
                                DataType.get(schema[i]),
                                DataType.get(current.type)));
            }

            fillOneGPDBWritableField(current, i);
        }

        outputList.add(output);
    }

    /**
     * Tests if data type is a string type. String type is a type that can be
     * serialized as string, such as varchar, bpchar, text, numeric, timestamp,
     * date.
     *
     * @param type data type
     * @return whether data type is string type
     */
    boolean isStringType(DataType type) {
        if (type.isArrayType()) {
           return DataType.isTextForm(type.getTypeElem().getOID());
        } else {
            return DataType.isTextForm(type.getOID());
        }
    }

    /**
     * Tests if record field type and schema type correspond.
     *
     * @param recType    record type code
     * @param schemaType schema type code
     * @return whether record type and schema type match
     */
    boolean isTypeInSchema(int recType, int schemaType) {
        DataType dtRec = DataType.get(recType);
        // schema from GPDB table
        DataType dtSchema = DataType.get(schemaType);

        return (dtSchema == DataType.UNSUPPORTED_TYPE || dtRec == dtSchema
                || (isStringType(dtRec) && isStringType(dtSchema)));
    }

    /**
     * Fills a Text object based on recFields.
     *
     * @param recFields record fields
     * @throws BadRecordException if text formatted record has more than one
     *                            field
     */
    void fillText(List<OneField> recFields) throws BadRecordException {
        if (recFields.size() < 1)
            throw new BadRecordException(
                    "BridgeOutputBuilder must receive one field when handling the TEXT format");

        OneField field = recFields.get(0);
        Object val = field.val;
        DataType dataType = DataType.get(field.type);

        if (recFields.size() == 1 && dataType == DataType.BYTEA) {
            if (samplingEnabled) {
                convertTextDataToLines((byte[]) val);
                return;
            } else {
                // TODO break output into lines
                output = new BufferWritable((byte[]) val);
            }
        } else {

            if (recFields.size() == 1 && val instanceof org.apache.hadoop.io.Text) {
                org.apache.hadoop.io.Text text = (org.apache.hadoop.io.Text) val;
                text.append(newLineBytes, 0, newLineBytes.length);

                // Note that text.getLength() might be different from the size
                // of the underlying byte[] used by Text.
                // text.getBytes().length not necessarily equals to text.getLength()
                output = new BufferWritable(text.getBytes(), text.getLength());
            } else if (recFields.size() == 1 && val instanceof String) {
                output = new Text(val + newLine);
            } else {
                output = new Text(fieldListToCSVString(recFields));
            }

            // append magic header?

        }

        outputList.add(output);
    }

    /**
     * Breaks raw bytes into lines. Used only for sampling.
     * <p>
     * When sampling a data source, we have to make sure that we deal with
     * actual rows (lines) and not bigger chunks of data such as used by
     * LineBreakAccessor for performance. The input byte array is broken into
     * lines, each one stored in the outputList. In case the read data doesn't
     * end with a line delimiter, which can happen when reading chunks of bytes,
     * the partial line is stored separately, and is being completed when
     * reading the next chunk of data.
     *
     * @param val input raw data to break into lines
     */
    void convertTextDataToLines(byte[] val) {
        int len = val.length;
        int start = 0;
        int end;
        byte[] line;
        BufferWritable writable;

        while (start < len) {
            end = ArrayUtils.indexOf(val, DELIM, start);
            boolean isPartialLine;
            if (end == ArrayUtils.INDEX_NOT_FOUND) {
                // data finished in the middle of the line
                end = len;
                isPartialLine = true;
            } else {
                end++; // include the DELIM character
                isPartialLine = false;
            }
            line = Arrays.copyOfRange(val, start, end);

            if (partialLine != null) {
                // partial data was completed
                ((BufferWritable) partialLine).append(line);
                writable = (BufferWritable) partialLine;
                partialLine = null;
            } else {
                writable = new BufferWritable(line);
            }

            if (isPartialLine) {
                partialLine = writable;
            } else {
                outputList.add(writable);
            }
            start = end;
        }
    }

    /**
     * Fills one GPDBWritable field.
     *
     * @param oneField field
     * @param colIdx   column index
     * @throws BadRecordException if field type is not supported or doesn't
     *                            match the schema
     */
    void fillOneGPDBWritableField(OneField oneField, int colIdx)
            throws BadRecordException {
        int type = oneField.type;
        Object val = oneField.val;
        GPDBWritable gpdbOutput = (GPDBWritable) output;
        try {
            switch (DataType.get(type)) {
                case INTEGER:
                    gpdbOutput.setInt(colIdx, (Integer) val);
                    break;
                case FLOAT8:
                    gpdbOutput.setDouble(colIdx, (Double) val);
                    break;
                case REAL:
                    gpdbOutput.setFloat(colIdx, (Float) val);
                    break;
                case BIGINT:
                    gpdbOutput.setLong(colIdx, (Long) val);
                    break;
                case SMALLINT:
                    gpdbOutput.setShort(colIdx, (Short) val);
                    break;
                case BOOLEAN:
                    gpdbOutput.setBoolean(colIdx, (Boolean) val);
                    break;
                case BYTEA:
                    byte[] bts = null;
                    if (val != null) {
                        int length = Array.getLength(val);
                        bts = new byte[length];
                        for (int j = 0; j < length; j++) {
                            bts[j] = Array.getByte(val, j);
                        }
                    }
                    gpdbOutput.setBytes(colIdx, bts);
                    break;
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                case UUID:
                case NUMERIC:
                case TIMESTAMP:
                case TIMESTAMP_WITH_TIME_ZONE:
                case TIME:
                case DATE:
                case BOOLARRAY:
                case BYTEAARRAY:
                case INT2ARRAY:
                case INT4ARRAY:
                case INT8ARRAY:
                case FLOAT4ARRAY:
                case FLOAT8ARRAY:
                case TEXTARRAY:
                case BPCHARARRAY:
                case VARCHARARRAY:
                case DATEARRAY:
                case UUIDARRAY:
                case NUMERICARRAY:
                case TIMEARRAY:
                case TIMESTAMPARRAY:
                case TIMESTAMP_WITH_TIMEZONE_ARRAY:
                    /*
                     * If resolvers support sending arrays to GPDB, they are expected to serialize arrays into Postgres
                     * array external text representation.
                     * see https://www.postgresql.org/docs/9.4/arrays.html for details of this format.
                     */
                    gpdbOutput.setString(colIdx,
                            ObjectUtils.toString(val, null));
                    break;
                default:
                    LOG.debug("Data type OID is {}", type);
                    String valClassName = (val != null) ? val.getClass().getSimpleName()
                            : null;
                    throw new UnsupportedOperationException(valClassName
                            + " is not supported for GPDB conversion");
            }
        } catch (GPDBWritable.TypeMismatchException e) {
            throw new BadRecordException(e);
        }
    }

    /**
     * Serialize a list of OneFields to a CSV line
     *
     * @param fields list of fields
     * @return a serialized CSV line
     */
    private String fieldListToCSVString(List<OneField> fields) {
        return fields.stream()
                .map(field -> {
                    // Check first if the field.val is null then using .toString() is safe in else branches.
                    if (field.val == null) {
                        return greenplumCSV.getValueOfNull();
                    } else if (field.type == DataType.BYTEA.getOID()) {
                        // check for Format Type here. if the Format Type is CSV, we should escape using single \
                        // for Text or Custom Format types, it should \\
                        String hexPrepend = gpdbTableformat.equalsIgnoreCase("csv") ? "\\x" : "\\\\x";
                        return hexPrepend + Hex.encodeHexString((byte[]) field.val);
                    } else if (field.type == DataType.NUMERIC.getOID() || !DataType.isTextForm(field.type)) {
                        return field.val.toString();
                    } else if (field.type == DataType.TIMESTAMP.getOID()) {
                        return field.val instanceof String ?
                                (String) field.val :
                                ((Timestamp) field.val).toLocalDateTime().format(GreenplumDateTime.DATETIME_FORMATTER);
                    } else if (field.type == DataType.DATE.getOID()) {
                        return field.val.toString();
                    } else
                        return greenplumCSV.toCsvField(field.val.toString(), true, true, true);
                })
                .collect(Collectors.joining(String.valueOf(greenplumCSV.getDelimiter()), "", newLine));
    }
}
