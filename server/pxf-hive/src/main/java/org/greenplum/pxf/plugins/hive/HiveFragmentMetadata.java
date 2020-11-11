package org.greenplum.pxf.plugins.hive;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.mapred.FileSplit;
import org.greenplum.pxf.plugins.hdfs.HcfsFragmentMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;

/**
 * Fragment Metadata for Hive
 */
@NoArgsConstructor
public class HiveFragmentMetadata extends HcfsFragmentMetadata implements KryoSerializable {

    private static final Logger LOG = LoggerFactory.getLogger(HiveFragmentMetadata.class);

    /**
     * A list of common file input format class names found in PXF
     */
    public static final String[] KNOWN_FILE_INPUT_FORMATS = {
            org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat.class.getName(),
            org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.class.getName(),
            org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat.class.getName(),
            org.apache.hadoop.hive.ql.io.RCFileInputFormat.class.getName(),
            org.apache.hadoop.mapred.SequenceFileInputFormat.class.getName(),
            org.apache.hadoop.mapred.TextInputFormat.class.getName()
    };

    /**
     * A list of common serialization libraries class names found in PXF
     */
    public static final String[] KNOWN_SERIALIZATION_LIBS = {
            org.apache.hadoop.hive.ql.io.orc.OrcSerde.class.getName(),
            org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe.class.getName(),
            org.apache.hadoop.hive.serde2.avro.AvroSerDe.class.getName(),
            org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class.getName(),
            org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe.class.getName(),
            org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName(),
            org.apache.hadoop.hive.serde2.OpenCSVSerde.class.getName(),
    };

    /**
     * Properties needed for SerDe initialization
     */
    @Getter
    private Properties properties;

    /**
     * Default constructor for JSON serialization
     */
    public HiveFragmentMetadata(long start, long length, Properties properties) {
        super(start, length);
        this.properties = properties;
    }

    /**
     * Constructs a {@link HiveFragmentMetadata} object with the given
     * {@code fileSplit} and the {@code properties}.
     *
     * @param fileSplit  the {@link FileSplit} object.
     * @param properties the properties
     */
    public HiveFragmentMetadata(FileSplit fileSplit, Properties properties) {
        super(fileSplit);
        this.properties = properties;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeLong(getStart(), true);
        output.writeLong(getLength(), true);

        int length = properties.size();
        output.writeInt(length, true);

        for (String key : properties.stringPropertyNames()) {
            output.writeString(key);
            String value = properties.getProperty(key);

            if (FILE_INPUT_FORMAT.equals(key)) {
                output.writeString(replaceValue(value, KNOWN_FILE_INPUT_FORMATS));
            } else if (SERIALIZATION_LIB.equals(key)) {
                output.writeString(replaceValue(value, KNOWN_SERIALIZATION_LIBS));
            } else {
                output.writeString(value);
            }
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        start = input.readLong(true);
        length = input.readLong(true);
        properties = new Properties();

        int length = input.readInt(true);
        for (int i = 0; i < length; i++) {
            String key = input.readString();
            String value = input.readString();

            if (FILE_INPUT_FORMAT.equals(key)) {
                properties.put(key, replaceIndex(value, KNOWN_FILE_INPUT_FORMATS));
            } else if (SERIALIZATION_LIB.equals(key)) {
                properties.put(key, replaceIndex(value, KNOWN_SERIALIZATION_LIBS));
            } else {
                properties.put(key, value);
            }
        }
    }

    /**
     * Returns the index of the value in the given array of values, if the
     * value is not found it returns the original value.
     *
     * @param value  the value to replace
     * @param values the array of known values
     * @return the index of the value in the array of values, or the original
     * value when the values does not contain the value
     */
    private String replaceValue(String value, String[] values) {
        for (int i = 0; i < values.length; i++) {
            if (value.equals(values[i])) {
                return Integer.toString(i);
            }
        }
        return value;
    }

    /**
     * Returns the value that we look up in the values array, if the value
     * provided is not an integer, we return the original value. Note that
     * this can cause an ArrayIndexOutOfBoundsException, but we don't expect
     * this to be a realistic scenario
     *
     * @param value  the value as an integer
     * @param values the array of known values
     * @return the mapped value from the given array of values
     */
    private String replaceIndex(String value, String[] values) {
        try {
            int index = Integer.parseInt(value);
            // assume the index exists
            return values[index];
        } catch (NumberFormatException e) {
            LOG.debug("Unable to parse value {}", value);
        }
        return value;
    }
}
