package org.greenplum.pxf.plugins.hdfs.orc;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.SupportedOperatorPruner;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.HcfsType;
import org.greenplum.pxf.plugins.hdfs.filter.BPCharOperatorTransformer;
import org.greenplum.pxf.plugins.hdfs.filter.SearchArgumentBuilder;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class ORCVectorizedAccessor extends BasePlugin implements Accessor {

    public static final EnumSet<Operator> SUPPORTED_OPERATORS =
            EnumSet.of(
                    Operator.NOOP,
                    Operator.LESS_THAN,
                    Operator.GREATER_THAN,
                    Operator.LESS_THAN_OR_EQUAL,
                    Operator.GREATER_THAN_OR_EQUAL,
                    Operator.EQUALS,
                    Operator.NOT_EQUALS,
                    // Operator.LIKE,
                    Operator.IS_NULL,
                    Operator.IS_NOT_NULL,
                    Operator.IN,
                    Operator.AND,
                    Operator.OR,
                    Operator.NOT
            );
    private static final TreeVisitor PRUNER = new SupportedOperatorPruner(SUPPORTED_OPERATORS);
    private static final TreeTraverser TRAVERSER = new TreeTraverser();

    private static final String ORC_FILE_SUFFIX = ".orc";
    static final String MAP_BY_POSITION_OPTION = "MAP_BY_POSITION";

    /**
     * True if the accessor accesses the columns defined in the
     * ORC file in the same order they were defined in the Greenplum table,
     * otherwise the columns are matches by name. (Defaults to false)
     */
    private boolean positionalAccess;
    private int batchIndex;
    private long totalRowsRead;
    private long totalReadTimeInNanos;
    private Reader fileReader;
    private RecordReader recordReader;
    private VectorizedRowBatch batch;
    private List<ColumnDescriptor> columnDescriptors;

    // -- write properties
    private ORCSchemaBuilder schemaBuilder;
    private Writer fileWriter;

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        columnDescriptors = context.getTupleDescription();
        positionalAccess = context.getOption(MAP_BY_POSITION_OPTION, false);
    }

    @Override
    public boolean openForRead() throws IOException {
        Path file = new Path(context.getDataSource());
        FileSplit fileSplit = HdfsUtilities.parseFileSplit(context.getDataSource(), context.getFragmentMetadata());

        fileReader = OrcFile.createReader(file, OrcFile
                .readerOptions(configuration)
                .filesystem(file.getFileSystem(configuration)));

        // The original schema from the file
        TypeDescription schema = fileReader.getSchema();
        // Add column projection to the Reader.Options
        TypeDescription readSchema = buildReadSchema(schema);
        // Get the record filter in case of predicate push-down
        SearchArgument searchArgument = getSearchArgument(context.getFilterString(), schema);

        // Build the reader options
        Reader.Options options = fileReader
                .options()
                .schema(readSchema)
                .positionalEvolutionLevel(0)
                .range(fileSplit.getStart(), fileSplit.getLength())
                .searchArgument(searchArgument, new String[]{});

        // Read the row data
        final Instant start = Instant.now();
        recordReader = fileReader.rows(options);
        batch = readSchema.createRowBatch();
        // Keep track of time here since the fileReader.rows call will read data
        totalReadTimeInNanos += Duration.between(start, Instant.now()).toNanos();
        context.setMetadata(readSchema);
        return true;
    }

    /**
     * Reads the next batch for the current fragment
     *
     * @return the next batch in OneRow format, the key is the batch number, and data is the batch
     * @throws IOException when reading of the next batch occurs
     */
    @Override
    public OneRow readNextObject() throws IOException {
        final Instant start = Instant.now();
        final boolean hasNextBatch = recordReader.nextBatch(batch);
        totalReadTimeInNanos += Duration.between(start, Instant.now()).toNanos();
        if (hasNextBatch) {
            totalRowsRead += batch.size;
            return new OneRow(new LongWritable(batchIndex++), batch);
        }
        return null; // all batches are exhausted
    }

    @Override
    public void closeForRead() throws IOException {
        logReadStats(totalRowsRead, totalReadTimeInNanos);
        if (recordReader != null) {
            recordReader.close();
        }
        if (fileReader != null) {
            fileReader.close();
        }
    }

    @Override
    public boolean openForWrite() throws IOException {
        HcfsType hcfsType = HcfsType.getHcfsType(context);
        // ORC does not use codec suffix in filenames
        String fileName = hcfsType.getUriForWrite(context) + ORC_FILE_SUFFIX;
        //TODO: figure out compression settings
        /*
        check Spark OrcUtils.extensionsForCompressionCodecNames(..)
        val extensionsForCompressionCodecNames = Map(
                "NONE" -> "",
                "SNAPPY" -> ".snappy",
                "ZLIB" -> ".zlib",
                "LZO" -> ".lzo")
         */
        //String compressCodec = context.getOption("COMPRESSION_CODEC");

        // TODO: process any PXF options for ORC write

        Path file = new Path(fileName);
        schemaBuilder = new ORCSchemaBuilder();
        TypeDescription writeSchema = schemaBuilder.buildSchema(context.getTupleDescription());
        fileWriter = OrcFile.createWriter(file, OrcFile.writerOptions(configuration).setSchema(writeSchema));

        context.setMetadata(writeSchema);
        return true;
    }

    @Override
    public boolean writeNextObject(OneRow onerow) throws IOException {
        // get a row batch produced by the resolver
        // TODO: do we want to create just once and re-use it ?
        VectorizedRowBatch rowBatch = (VectorizedRowBatch) onerow.getData();
        fileWriter.addRowBatch(rowBatch);
        rowBatch.reset(); // TODO: need to reset only if the batch will be reused
        return true;
    }

    @Override
    public void closeForWrite() throws IOException {
        if (fileWriter != null) {
            fileWriter.close();
        }
    }

    /**
     * Given a filter string, builds the SearchArgument object to perform
     * predicated pushdown for ORC
     *
     * @param filterString   the serialized filter string from the query predicate
     * @param originalSchema the original schema for the ORC file
     * @return null if filter string is null, the built SearchArgument otherwise
     * @throws IOException when a filter parsing error occurs
     */
    private SearchArgument getSearchArgument(String filterString, TypeDescription originalSchema) throws IOException {
        if (StringUtils.isBlank(filterString)) {
            return null;
        }

        List<ColumnDescriptor> descriptors = columnDescriptors;

        if (positionalAccess) {
            // We need to adjust the descriptors to match the column names
            // in the ORC schema to support predicate push down
            descriptors = new ArrayList<>();
            for (int i = 0; i < columnDescriptors.size() && i < originalSchema.getFieldNames().size(); i++) {
                ColumnDescriptor columnDescriptor = columnDescriptors.get(i);
                String columnName = originalSchema.getFieldNames().get(i);
                ColumnDescriptor copyDescriptor = new ColumnDescriptor(
                        columnName, // the name of the column in the ORC schema
                        columnDescriptor.columnTypeCode(),
                        columnDescriptor.columnIndex(),
                        columnDescriptor.columnTypeName(),
                        columnDescriptor.columnTypeModifiers(),
                        columnDescriptor.isProjected());
                descriptors.add(copyDescriptor);
            }
        }

        SearchArgumentBuilder searchArgumentBuilder =
                new SearchArgumentBuilder(descriptors, configuration);

        TreeVisitor bpCharOperatorTransformer = new BPCharOperatorTransformer(descriptors);

        // Parse the filter string into a expression tree Node
        Node root = new FilterParser().parse(filterString);
        // Prune the parsed tree with valid supported operators and then
        // traverse the pruned tree with the searchArgumentBuilder to produce a
        // SearchArgument for ORC
        TRAVERSER.traverse(root, PRUNER, bpCharOperatorTransformer, searchArgumentBuilder);

        // Build the SearchArgument object
        return searchArgumentBuilder.getFilterBuilder().build();
    }

    /**
     * Given the column descriptors that we receive from Greenplum, builds
     * the read schema that will perform column projection
     *
     * @param originalSchema the original schema for the ORC file
     * @return the read schema
     */
    private TypeDescription buildReadSchema(TypeDescription originalSchema) {
        TypeDescription readSchema = TypeDescription.createStruct();
        if (positionalAccess) {
            for (int i = 0; i < columnDescriptors.size() && i < originalSchema.getFieldNames().size(); i++) {
                ColumnDescriptor columnDescriptor = columnDescriptors.get(i);
                if (!columnDescriptor.isProjected()) continue;
                String columnName = originalSchema.getFieldNames().get(i);
                TypeDescription t = originalSchema.getChildren().get(i);
                readSchema.addField(columnName, t.clone());
            }
        } else {
            int schemaSize = originalSchema.getFieldNames().size();
            // Build a map of column names to TypeDescription
            // We need to add the original name and lower cased name to
            // the map to support mixed case where in Greenplum the column name
            // was created with quotes i.e "mIxEd CaSe". When quotes are not
            // used to create a table in Greenplum, the name of the column will
            // always come in lower-case
            Map<String, TypeDescription> originalFields = new HashMap<>(schemaSize);
            IntStream.range(0, schemaSize).forEach(idx -> {
                String columnName = originalSchema.getFieldNames().get(idx);
                TypeDescription t = originalSchema.getChildren().get(idx);
                originalFields.put(columnName, t);
                originalFields.put(columnName.toLowerCase(), t);
            });

            for (ColumnDescriptor columnDescriptor : columnDescriptors) {
                if (!columnDescriptor.isProjected()) continue;
                String columnName = columnDescriptor.columnName();
                TypeDescription t = originalFields.get(columnName);
                if (t != null) {
                    readSchema.addField(columnName, t.clone());
                }
            }
        }
        return readSchema;
    }
}
