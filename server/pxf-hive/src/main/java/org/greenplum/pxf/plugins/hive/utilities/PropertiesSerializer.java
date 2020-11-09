package org.greenplum.pxf.plugins.hive.utilities;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;

/**
 * This class optimizes the serialization of metastore Properties by encoding
 * the known file input formats and known serialization libraries.
 * <p>
 * During the serialization process, the property values for the file input
 * format and serialization library are replaced with the value of the index
 * of the given array.
 * <p>
 * For the deserialization process, if the value of the file input format
 * or serialization library is an integer, we use that integer as a lookup
 * in the array for the given property.
 * <p>
 * The list of known file input format and serialization libs does not include
 * all possible values, but it is a list of the most frequently values for
 * PXF.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class PropertiesSerializer extends MapSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(PropertiesSerializer.class);

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

    public PropertiesSerializer() {
        setKeyClass(String.class, null);
        setValueClass(String.class, null);
    }

    /**
     * Creates a copy of the map, and replaces the values for the
     * file input format and serialization library with the index in the array
     * of the given property
     *
     * @param kryo   the kryo used for write
     * @param output the output stream
     * @param map    the map to be serialized
     */
    @Override
    public void write(Kryo kryo, Output output, Map map) {
        Map mapCopy = new HashMap(map);
        // Replace values with their index in the array
        replaceValue(mapCopy, FILE_INPUT_FORMAT, KNOWN_FILE_INPUT_FORMATS);
        replaceValue(mapCopy, SERIALIZATION_LIB, KNOWN_SERIALIZATION_LIBS);
        super.write(kryo, output, mapCopy);
    }

    /**
     * Deserializes the map and replaces indices for the given properties into
     * the class names for the file input format and the serialization
     * libraries
     *
     * @param kryo  the kryo used for read
     * @param input the input stream
     * @param type  the class type
     * @return the map
     */
    @Override
    public Map read(Kryo kryo, Input input, Class<Map> type) {
        Map map = super.read(kryo, input, type);
        // Replace indices with their corresponding value in the array
        replaceIndex(map, FILE_INPUT_FORMAT, KNOWN_FILE_INPUT_FORMATS);
        replaceIndex(map, SERIALIZATION_LIB, KNOWN_SERIALIZATION_LIBS);
        return map;
    }

    /**
     * When the value for a given key is found in the values array, it replaces
     * the value with the corresponding index in the array. When the value is
     * not found, no action is taken
     *
     * @param map    the map
     * @param key    the key to replace
     * @param values the array of known values
     */
    private void replaceValue(Map map, String key, String[] values) {
        String value;
        if ((value = (String) map.get(key)) == null) return;

        for (int i = 0; i < values.length; i++) {
            if (value.equals(values[i])) {
                map.put(key, Integer.toString(i));
                break;
            }
        }
    }

    /**
     * When the value for the given key is an integer, we use the integer
     * to look up the value in the array. If the value for the given key is
     * not an integer, no action is taken. Note that this can cause an
     * ArrayIndexOutOfBoundsException, but we don't expect this to be a
     * realistic scenario
     *
     * @param map    the map
     * @param key    the key to replace
     * @param values the array of known values
     */
    private void replaceIndex(Map map, String key, String[] values) {
        String value;
        if ((value = (String) map.get(key)) == null) return;

        try {
            int index = Integer.parseInt(value);
            // assume the index exists
            map.put(key, values[index]);
        } catch (NumberFormatException e) {
            LOG.debug("Unable to parse value {}", value);
        }
    }
}
