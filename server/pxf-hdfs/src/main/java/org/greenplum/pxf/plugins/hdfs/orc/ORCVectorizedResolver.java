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
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.greenplum.pxf.api.io.DataType.BIGINT;
import static org.greenplum.pxf.api.io.DataType.BOOLEAN;
import static org.greenplum.pxf.api.io.DataType.BPCHAR;
import static org.greenplum.pxf.api.io.DataType.BYTEA;
import static org.greenplum.pxf.api.io.DataType.DATE;
import static org.greenplum.pxf.api.io.DataType.FLOAT8;
import static org.greenplum.pxf.api.io.DataType.INTEGER;
import static org.greenplum.pxf.api.io.DataType.NUMERIC;
import static org.greenplum.pxf.api.io.DataType.REAL;
import static org.greenplum.pxf.api.io.DataType.SMALLINT;
import static org.greenplum.pxf.api.io.DataType.TEXT;
import static org.greenplum.pxf.api.io.DataType.TIMESTAMP;
import static org.greenplum.pxf.api.io.DataType.VARCHAR;
import static org.greenplum.pxf.plugins.hdfs.orc.ORCVectorizedAccessor.MAP_BY_POSITION_OPTION;

/**
 * Resolves ORC VectorizedRowBatch into lists of List<OneField>. Only primitive
 * types are supported. Currently, Timestamp and Timestamp with TimeZone are
 * not supported. The supported mapping is as follows:
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
 */
public class ORCVectorizedResolver extends BasePlugin implements ReadVectorizedResolver, Resolver {

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
                } else if (orcColumn.getCategory().isPrimitive()) {
                    oneFields = functions[columnIndex]
                            .apply(vectorizedBatch, vectorizedBatch.cols[columnIndex], typeOidMappings[columnIndex]);
                    columnIndex++;
                } else {
                    throw new UnsupportedTypeException(
                            String.format("Unable to resolve column '%s' with category '%s'. Only primitive types are supported.",
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
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OneRow setFields(List<OneField> record) {
        throw new UnsupportedOperationException();
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
            // Reset the internal lists
            for (int i = 0; i < batchSize; i++) {
                // clear does not reclaim back the internal arrays of the arraylists
                // this is what we prefer
                cachedBatch.get(i).clear();
            }
        }

        return cachedBatch;
    }
}
