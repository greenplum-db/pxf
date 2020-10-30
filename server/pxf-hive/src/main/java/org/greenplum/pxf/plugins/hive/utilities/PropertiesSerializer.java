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

@SuppressWarnings({"unchecked", "rawtypes"})
public class PropertiesSerializer extends MapSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(PropertiesSerializer.class);

    public static final String[] KNOWN_FILE_INPUT_FORMATS = {
            org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.class.getName(),
            org.apache.hadoop.hive.ql.io.RCFileInputFormat.class.getName(),
            org.apache.hadoop.mapred.TextInputFormat.class.getName(),
            org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat.class.getName(),
            org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat.class.getName(),
            org.apache.hadoop.mapred.SequenceFileInputFormat.class.getName()
    };

    public static final String[] KNOWN_SERIALIZATION_LIBS = {
            org.apache.hadoop.hive.ql.io.orc.OrcSerde.class.getName(),
            org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class.getName(),
            org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe.class.getName(),
            org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe.class.getName(),
            org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName(),
            org.apache.hadoop.hive.serde2.avro.AvroSerDe.class.getName(),
            org.apache.hadoop.hive.serde2.OpenCSVSerde.class.getName(),
    };

    public PropertiesSerializer() {
        setKeyClass(String.class, null);
        setValueClass(String.class, null);
    }

    @Override
    public void write(Kryo kryo, Output output, Map map) {
        Map mapCopy = new HashMap(map);
        // Replace values with their index in the array
        replaceValue(mapCopy, FILE_INPUT_FORMAT, KNOWN_FILE_INPUT_FORMATS);
        replaceValue(mapCopy, SERIALIZATION_LIB, KNOWN_SERIALIZATION_LIBS);
        super.write(kryo, output, mapCopy);
    }

    @Override
    public Map read(Kryo kryo, Input input, Class<Map> type) {
        Map map = super.read(kryo, input, type);
        // Replace indices with their corresponding value in the array
        replaceIndex(map, FILE_INPUT_FORMAT, KNOWN_FILE_INPUT_FORMATS);
        replaceIndex(map, SERIALIZATION_LIB, KNOWN_SERIALIZATION_LIBS);
        return map;
    }

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
