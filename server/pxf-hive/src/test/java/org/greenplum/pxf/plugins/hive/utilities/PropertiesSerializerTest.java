package org.greenplum.pxf.plugins.hive.utilities;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PropertiesSerializerTest {

    static Stream<Object[]> inputFormatValues() {
        return Stream.of(
                new Object[]{"org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat", "0"},
                new Object[]{"org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "1"},
                new Object[]{"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", "2"},
                new Object[]{"org.apache.hadoop.hive.ql.io.RCFileInputFormat", "3"},
                new Object[]{"org.apache.hadoop.mapred.SequenceFileInputFormat", "4"},
                new Object[]{"org.apache.hadoop.mapred.TextInputFormat", "5"},
                new Object[]{"not-in-map", "not-in-map"},
                new Object[]{"dont-serialize-me", "dont-serialize-me"}
        );
    }

    static Stream<Object[]> serializationValues() {
        return Stream.of(
                new Object[]{"org.apache.hadoop.hive.ql.io.orc.OrcSerde", "0"},
                new Object[]{"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", "1"},
                new Object[]{"org.apache.hadoop.hive.serde2.avro.AvroSerDe", "2"},
                new Object[]{"org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe", "3"},
                new Object[]{"org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe", "4"},
                new Object[]{"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", "5"},
                new Object[]{"org.apache.hadoop.hive.serde2.OpenCSVSerde", "6"},
                new Object[]{"not-in-the-list", "not-in-the-list"});
    }

    protected Kryo propertiesKryo;
    protected Kryo kryo;

    @BeforeEach
    public void setup() {
        propertiesKryo = new Kryo();
        propertiesKryo.addDefaultSerializer(Map.class, PropertiesSerializer.class);

        kryo = new Kryo();
    }

    @MethodSource("inputFormatValues")
    @ParameterizedTest
    public void testFileInputFormatSerialization(String fileInputFormat, String expectedFileInputFormat) {
        Properties properties = new Properties();
        properties.put("file.inputformat", fileInputFormat);

        Output out = new Output(4 * 1024, 10 * 1024 * 1024);
        propertiesKryo.writeObject(out, properties);
        out.close();

        byte[] bytes = out.toBytes();
        Properties propertiesResult = kryo.readObject(new Input(bytes), Properties.class);
        assertEquals(expectedFileInputFormat, propertiesResult.get("file.inputformat"));
    }

    @MethodSource("serializationValues")
    @ParameterizedTest
    public void testSerializationLibSerialization(String serializationLib, String expectedSerializationLib) {
        Properties properties = new Properties();
        properties.put("serialization.lib", serializationLib);

        Output out = new Output(4 * 1024, 10 * 1024 * 1024);
        propertiesKryo.writeObject(out, properties);
        out.close();

        byte[] bytes = out.toBytes();
        Properties propertiesResult = kryo.readObject(new Input(bytes), Properties.class);
        assertEquals(expectedSerializationLib, propertiesResult.get("serialization.lib"));
    }

    @MethodSource("inputFormatValues")
    @ParameterizedTest
    public void testFileInputFormatDeserialization(String fileInputFormat, String expectedFileInputFormat) {
        Properties properties = new Properties();
        properties.put("file.inputformat", expectedFileInputFormat);

        Output out = new Output(4 * 1024, 10 * 1024 * 1024);
        propertiesKryo.writeObject(out, properties);
        out.close();

        byte[] bytes = out.toBytes();
        Properties propertiesResult = propertiesKryo.readObject(new Input(bytes), Properties.class);
        assertEquals(fileInputFormat, propertiesResult.get("file.inputformat"));
    }

    @MethodSource("serializationValues")
    @ParameterizedTest
    public void testSerializationLibDeserialization(String serializationLib, String expectedSerializationLib) {
        Properties properties = new Properties();
        properties.put("serialization.lib", expectedSerializationLib);

        Output out = new Output(4 * 1024, 10 * 1024 * 1024);
        propertiesKryo.writeObject(out, properties);
        out.close();

        byte[] bytes = out.toBytes();
        Properties propertiesResult = propertiesKryo.readObject(new Input(bytes), Properties.class);
        assertEquals(serializationLib, propertiesResult.get("serialization.lib"));
    }

}