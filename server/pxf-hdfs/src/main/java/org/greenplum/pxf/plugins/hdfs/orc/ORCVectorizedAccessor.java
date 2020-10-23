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
import org.greenplum.pxf.plugins.hdfs.filter.SearchArgumentBuilder;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
                    Operator.IS_NULL,
                    Operator.IS_NOT_NULL,
                    Operator.IN,
                    Operator.OR,
                    Operator.AND,
                    Operator.NOT
            );
    private static final TreeVisitor PRUNER = new SupportedOperatorPruner(SUPPORTED_OPERATORS);
    private static final TreeTraverser TRAVERSER = new TreeTraverser();

    private boolean positionalAccess;
    private int batchIndex;
    private long totalRowsRead;
    private long totalReadTimeInNanos;
    private Reader fileReader;
    private RecordReader recordReader;
    private VectorizedRowBatch batch;
    private List<ColumnDescriptor> columnDescriptors;

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        columnDescriptors = context.getTupleDescription();
        positionalAccess = context.getOption("MAP_BY_POSITION", false);
    }

    @Override
    public boolean openForRead() throws IOException {
        Path file = new Path(context.getDataSource());
        FileSplit fileSplit = HdfsUtilities.parseFileSplit(context);

        fileReader = OrcFile.createReader(file, OrcFile
                .readerOptions(configuration)
                .filesystem(file.getFileSystem(configuration)));

        // The original schema from the file
        TypeDescription schema = fileReader.getSchema();
        // Add column projection to the Reader.Options
        TypeDescription readSchema = buildReadSchema(schema);
        // Get the record filter in case of predicate push-down
        SearchArgument searchArgument = getSearchArgument(context.getFilterString());

        // Build the reader options
        Reader.Options options = fileReader
                .options()
                .schema(readSchema)
                .positionalEvolutionLevel(0)
                .range(fileSplit.getStart(), fileSplit.getLength())
                .searchArgument(searchArgument, new String[]{});

        // Read the row data
        recordReader = fileReader.rows(options);
        batch = readSchema.createRowBatch();
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
        if (LOG.isDebugEnabled()) {
            final long millis = TimeUnit.NANOSECONDS.toMillis(totalReadTimeInNanos);
            long average = totalReadTimeInNanos / totalRowsRead;
            LOG.debug("{}-{}: Read TOTAL of {} rows from file {} on server {} in {} ms. Average speed: {} nanoseconds",
                    context.getTransactionId(),
                    context.getSegmentId(),
                    totalRowsRead,
                    context.getDataSource(),
                    context.getServerName(),
                    millis,
                    average);
        }
        if (recordReader != null) {
            recordReader.close();
        }
        if (fileReader != null) {
            fileReader.close();
        }
    }

    @Override
    public boolean openForWrite() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean writeNextObject(OneRow onerow) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeForWrite() {
        throw new UnsupportedOperationException();
    }

    /**
     * Given a filter string, builds the SearchArgument object to perform
     * predicated pushdown for ORC
     *
     * @param filterString the serialized filter string from the query predicate
     * @return null if filter string is null, the built SearchArgument otherwise
     * @throws IOException when a filter parsing error occurs
     */
    private SearchArgument getSearchArgument(String filterString) throws IOException {
        if (StringUtils.isBlank(filterString)) {
            return null;
        }

        SearchArgumentBuilder searchArgumentBuilder =
                new SearchArgumentBuilder(columnDescriptors, configuration);

        // Parse the filter string into a expression tree Node
        Node root = new FilterParser().parse(filterString);
        // Prune the parsed tree with valid supported operators and then
        // traverse the pruned tree with the searchArgumentBuilder to produce a
        // SearchArgument for ORC
        TRAVERSER.traverse(root, PRUNER, searchArgumentBuilder);

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
                if (t == null) {
                    throw new IllegalArgumentException(
                            String.format("Column %s is missing from ORC schema", columnName));
                }
                readSchema.addField(columnName, t.clone());
            }
        }
        return readSchema;
    }
}
