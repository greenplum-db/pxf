package org.greenplum.pxf.plugins.hdfs.orc;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.ReadVectorizedResolver;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.function.TriFunction;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.greenplum.pxf.api.io.DataType.BIGINT;
import static org.greenplum.pxf.api.io.DataType.BOOLARRAY;
import static org.greenplum.pxf.api.io.DataType.BOOLEAN;
import static org.greenplum.pxf.api.io.DataType.BPCHAR;
import static org.greenplum.pxf.api.io.DataType.BPCHARARRAY;
import static org.greenplum.pxf.api.io.DataType.BYTEA;
import static org.greenplum.pxf.api.io.DataType.BYTEAARRAY;
import static org.greenplum.pxf.api.io.DataType.DATE;
import static org.greenplum.pxf.api.io.DataType.FLOAT4ARRAY;
import static org.greenplum.pxf.api.io.DataType.FLOAT8;
import static org.greenplum.pxf.api.io.DataType.FLOAT8ARRAY;
import static org.greenplum.pxf.api.io.DataType.INT2ARRAY;
import static org.greenplum.pxf.api.io.DataType.INT4ARRAY;
import static org.greenplum.pxf.api.io.DataType.INT8ARRAY;
import static org.greenplum.pxf.api.io.DataType.INTEGER;
import static org.greenplum.pxf.api.io.DataType.NUMERIC;
import static org.greenplum.pxf.api.io.DataType.REAL;
import static org.greenplum.pxf.api.io.DataType.SMALLINT;
import static org.greenplum.pxf.api.io.DataType.TEXT;
import static org.greenplum.pxf.api.io.DataType.TEXTARRAY;
import static org.greenplum.pxf.api.io.DataType.TIMESTAMP;
import static org.greenplum.pxf.api.io.DataType.TIMESTAMP_WITH_TIME_ZONE;
import static org.greenplum.pxf.api.io.DataType.UNSUPPORTED_TYPE;
import static org.greenplum.pxf.api.io.DataType.VARCHAR;
import static org.greenplum.pxf.api.io.DataType.VARCHARARRAY;
import static org.greenplum.pxf.plugins.hdfs.orc.ORCVectorizedAccessor.MAP_BY_POSITION_OPTION;

/**
 * Resolves ORC VectorizedRowBatch into lists of List<OneField>. Currently,
 * Timestamp and Timestamp with TimeZone are not supported. The supported
 * scalar mapping is as follows:
 * <p>
 * ---------------------------------------------------------------------------
 * | ORC Physical Type | ORC Logical Type   | Greenplum Type | Greenplum OID |
 * ---------------------------------------------------------------------------
 * |  Long             |  boolean  (1 bit)  |  BOOLEAN       |  16           |
 * |  Long             |  tinyint  (8 bit)  |  SMALLINT      |  21           |
 * |  Long             |  smallint (16 bit) |  SMALLINT      |  21           |
 * |  Long             |  int      (32 bit) |  INTEGER       |  23           |
 * |  Long             |  bigint   (64 bit) |  BIGINT        |  20           |
 * |  Double           |  float             |  REAL          |  700          |
 * |  Double           |  double            |  FLOAT8        |  701          |
 * |  byte[]           |  string            |  TEXT          |  25           |
 * |  byte[]           |  char              |  BPCHAR        |  1042         |
 * |  byte[]           |  varchar           |  VARCHAR       |  1043         |
 * |  byte[]           |  binary            |  BYTEA         |  17           |
 * |  Long             |  date              |  DATE          |  1082         |
 * |  binary           |  decimal           |  NUMERIC       |  1700         |
 * |  binary           |  timestamp         |  TIMESTAMP     |  1114         |
 * ---------------------------------------------------------------------------
 *
 * Lists are the only supported compound type and only lists of the following scalar
 * types are supported. The supported compound mapping is as follows:
 * <p>
 * ------------------------------------------------------
 * | ORC Compound Type | Greenplum Type | Greenplum OID |
 * ------------------------------------------------------
 * | array<boolean>    | BOOLEAN[]      | 1000          |
 * | array<tinyint>    | SMALLINT[]     | 1005          |
 * | array<smallint>   | SMALLINT[]     | 1005          |
 * | array<int>        | INTEGER[]      | 1007          |
 * | array<bigint>     | BIGINT[]       | 1016          |
 * | array<float>      | FLOAT[]        | 1021          |
 * | array<double>     | FLOAT8[]       | 1022          |
 * | array<string>     | TEXT[]         | 1009          |
 * | array<char>       | BPCHAR[]       | 1014          |
 * | array<varchar>    | VARCHAR[]      | 1015          |
 * | array<binary>     | BYTEA[]        | 1001          |
 * ------------------------------------------------------
 */
public class ORCVectorizedResolver extends BasePlugin implements ReadVectorizedResolver, Resolver {

    private static final String UNSUPPORTED_ERR_MESSAGE = "Current operation is not supported";
    /**
     * The schema used to read the ORC file.
     */
    private TypeDescription readSchema;
    /**
     * An array of functions that resolve ColumnVectors into Lists of OneFields
     * The array has the same size as the readSchema, and the functions depend
     * on the type of the elements in the schema.
     */
    private TriFunction<VectorizedRowBatch, ColumnVector, Integer, OneField[]>[] functions;
    /**
     * An array of types that map from the readSchema types to Greenplum OIDs.
     */
    private int[] typeOidMappings;
    private Map<String, TypeDescription> readFields;
    /**
     * True if the resolver resolves the columns defined in the
     * ORC file in the same order they were defined in the Greenplum table,
     * otherwise the columns are matches by name. (Defaults to false)
     */
    private boolean positionalAccess;
    /**
     * A local copy of the column descriptors coming from the RequestContext.
     * We make this variable local to improve performance while accessing the
     * descriptors.
     */
    private List<ColumnDescriptor> columnDescriptors;
    private List<List<OneField>> cachedBatch;

    /**
     * {@inheritDoc}
     */
    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        columnDescriptors = context.getTupleDescription();
        positionalAccess = context.getOption(MAP_BY_POSITION_OPTION, false);
    }

    /**
     * Returns the resolved list of list of OneFields given a
     * VectorizedRowBatch
     *
     * @param batch unresolved batch
     * @return the resolved batch mapped to the Greenplum type
     */
    @Override
    public List<List<OneField>> getFieldsForBatch(OneRow batch) {
        ensureFunctionsAreInitialized();
        VectorizedRowBatch vectorizedBatch = (VectorizedRowBatch) batch.getData();
        int batchSize = vectorizedBatch.size;

        // The resolved batch returns a list of the list of OneField that
        // matches the size of the batch. Every internal list, has a list of
        // OneFields with size the number of columns
        List<List<OneField>> resolvedBatch = prepareResolvedBatch(batchSize);

        // index to the projected columns
        int columnIndex = 0;
        OneField[] oneFields;
        for (ColumnDescriptor columnDescriptor : columnDescriptors) {
            if (!columnDescriptor.isProjected()) {
                oneFields = ORCVectorizedMappingFunctions
                        .getNullResultSet(columnDescriptor.columnTypeCode(), batchSize);
            } else {
                TypeDescription orcColumn = positionalAccess
                        ? columnIndex < readSchema.getChildren().size() ? readSchema.getChildren().get(columnIndex) : null
                        : readFields.get(columnDescriptor.columnName());
                if (orcColumn == null) {
                    // this column is missing in the underlying ORC file, but
                    // it is defined in the Greenplum table. This can happen
                    // when a schema evolves, for example the original
                    // ORC-backed table had 4 columns, and at a later point in
                    // time a fifth column was added. Files written before the
                    // column was added will have 4 columns, and new files
                    // will have 5 columns
                    oneFields = ORCVectorizedMappingFunctions
                            .getNullResultSet(columnDescriptor.columnTypeCode(), batchSize);
                } else if (orcColumn.getCategory().isPrimitive() || orcColumn.getCategory() == TypeDescription.Category.LIST) {
                    oneFields = functions[columnIndex]
                            .apply(vectorizedBatch, vectorizedBatch.cols[columnIndex], typeOidMappings[columnIndex]);
                    columnIndex++;
                } else {
                    throw new UnsupportedTypeException(
                            String.format("Unable to resolve column '%s' with category '%s'. Only primitive and lists of primitive types are supported.",
                                    readSchema.getFieldNames().get(columnIndex), orcColumn.getCategory()));
                }
            }

            // oneFields is the array of fields for the current column we are
            // processing. We need to add it to the corresponding list
            for (int row = 0; row < batchSize; row++) {
                resolvedBatch.get(row).add(oneFields[row]);
            }
        }
        return resolvedBatch;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<OneField> getFields(OneRow row) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OneRow setFields(List<OneField> record) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
    }

    /**
     * Ensures that functions is initialized. If not initialized, it will
     * initialize the functions and typeOidMappings by iterating over the
     * readSchema, and building the mapping between ORC types to Greenplum
     * types.
     */
    @SuppressWarnings("unchecked")
    private void ensureFunctionsAreInitialized() {
        if (functions != null) return;
        if (!(context.getMetadata() instanceof TypeDescription))
            throw new PxfRuntimeException("No schema detected in request context");

        readSchema = (TypeDescription) context.getMetadata();
        int schemaSize = readSchema.getChildren().size();

        functions = new TriFunction[schemaSize];
        typeOidMappings = new int[schemaSize];

        readFields = new HashMap<>(schemaSize);
        IntStream.range(0, schemaSize).forEach(idx -> {
            String columnName = readSchema.getFieldNames().get(idx);
            TypeDescription t = readSchema.getChildren().get(idx);
            readFields.put(columnName, t);
            readFields.put(columnName.toLowerCase(), t);
        });

        List<TypeDescription> children = readSchema.getChildren();
        for (int i = 0; i < children.size(); i++) {
            TypeDescription t = children.get(i);
            switch (t.getCategory()) {
                case BOOLEAN:
                    functions[i] = ORCVectorizedMappingFunctions::booleanMapper;
                    typeOidMappings[i] = BOOLEAN.getOID();
                    break;
                case BYTE:
                case SHORT:
                    functions[i] = ORCVectorizedMappingFunctions::shortMapper;
                    typeOidMappings[i] = SMALLINT.getOID();
                    break;
                case INT:
                    functions[i] = ORCVectorizedMappingFunctions::integerMapper;
                    typeOidMappings[i] = INTEGER.getOID();
                    break;
                case LONG:
                    functions[i] = ORCVectorizedMappingFunctions::longMapper;
                    typeOidMappings[i] = BIGINT.getOID();
                    break;
                case FLOAT:
                    functions[i] = ORCVectorizedMappingFunctions::floatMapper;
                    typeOidMappings[i] = REAL.getOID();
                    break;
                case DOUBLE:
                    functions[i] = ORCVectorizedMappingFunctions::doubleMapper;
                    typeOidMappings[i] = FLOAT8.getOID();
                    break;
                case STRING:
                    functions[i] = ORCVectorizedMappingFunctions::textMapper;
                    typeOidMappings[i] = TEXT.getOID();
                    break;
                case DATE:
                    functions[i] = ORCVectorizedMappingFunctions::dateMapper;
                    typeOidMappings[i] = DATE.getOID();
                    break;
                case TIMESTAMP:
                    functions[i] = ORCVectorizedMappingFunctions::timestampMapper;
                    typeOidMappings[i] = TIMESTAMP.getOID();
                    break;
                case TIMESTAMP_INSTANT:
                    functions[i] = ORCVectorizedMappingFunctions::timestampWithTimeZoneMapper;
                    typeOidMappings[i] = TIMESTAMP_WITH_TIME_ZONE.getOID();
                    break;
                case BINARY:
                    functions[i] = ORCVectorizedMappingFunctions::binaryMapper;
                    typeOidMappings[i] = BYTEA.getOID();
                    break;
                case DECIMAL:
                    functions[i] = ORCVectorizedMappingFunctions::decimalMapper;
                    typeOidMappings[i] = NUMERIC.getOID();
                    break;
                case VARCHAR:
                    functions[i] = ORCVectorizedMappingFunctions::textMapper;
                    typeOidMappings[i] = VARCHAR.getOID();
                    break;
                case CHAR:
                    functions[i] = ORCVectorizedMappingFunctions::textMapper;
                    typeOidMappings[i] = BPCHAR.getOID();
                    break;
                case LIST:
                    functions[i] = ORCVectorizedMappingFunctions::listMapper;
                    typeOidMappings[i] = getArrayDataType(t.getChildren().get(0)).getOID();
                    break;
            }
        }
    }

    private List<List<OneField>> prepareResolvedBatch(int batchSize) {

        if (cachedBatch == null) {
            cachedBatch = new ArrayList<>(batchSize);

            // Initialize the internal lists
            for (int i = 0; i < batchSize; i++) {
                cachedBatch.add(new ArrayList<>(columnDescriptors.size()));
            }
        } else {
            // Need to be reallocated when batchSize is not equal to the size of the existing cacheBatch,
            // otherwise we will read more rows which we do not expect.
            if (batchSize != cachedBatch.size()) {
                cachedBatch = new ArrayList<>(batchSize);

                for (int i = 0; i < batchSize; i++) {
                    cachedBatch.add(new ArrayList<>(columnDescriptors.size()));
                }
            } else {
                // Reset the internal lists
                for (int i = 0; i < batchSize; i++) {
                    // clear does not reclaim back the internal arrays of the arraylists
                    // this is what we prefer
                    cachedBatch.get(i).clear();
                }
            }
        }

        return cachedBatch;
    }

    /**
     * This helper function returns the array DataType for the given primitive type
     *
     * @param typeDescription must be a child of a list TypeDescription
     * @return the DataType for the array containing elements of the given TypeDescription
     */
    private DataType getArrayDataType(TypeDescription typeDescription) {
        switch (typeDescription.getCategory()) {
            case BOOLEAN:
                return BOOLARRAY;
            case BYTE:
            case SHORT:
                return INT2ARRAY;
            case INT:
                return INT4ARRAY;
            case LONG:
                return INT8ARRAY;
            case FLOAT:
                return FLOAT4ARRAY;
            case DOUBLE:
                return FLOAT8ARRAY;
            case STRING:
                return TEXTARRAY;
            case VARCHAR:
                return VARCHARARRAY;
            case CHAR:
                return BPCHARARRAY;
            case BINARY:
                return BYTEAARRAY;
            case LIST:
                return getArrayDataType(typeDescription.getChildren().get(0));
            default:
                return UNSUPPORTED_TYPE;
        }
    }
}
