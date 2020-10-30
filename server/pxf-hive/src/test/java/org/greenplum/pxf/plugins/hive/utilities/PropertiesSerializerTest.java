package org.greenplum.pxf.plugins.hive.utilities;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class PropertiesSerializerTest {

    @Parameterized.Parameters
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat", "0",
                        "org.apache.hadoop.hive.ql.io.orc.OrcSerde", "0"},
                {"org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "1",
                        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", "1"},
                {"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", "2",
                        "org.apache.hadoop.hive.serde2.avro.AvroSerDe", "2"},
                {"org.apache.hadoop.hive.ql.io.RCFileInputFormat", "3",
                        "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe", "3"},
                {"org.apache.hadoop.mapred.SequenceFileInputFormat", "4",
                        "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe", "4"},
                {"org.apache.hadoop.mapred.TextInputFormat", "5",
                        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", "5"},
                {"not-in-map", "not-in-map",
                        "org.apache.hadoop.hive.serde2.OpenCSVSerde", "6"},
                {"dont-serialize-me", "dont-serialize-me",
                        "not-in-the-list", "not-in-the-list"}
        });
    }

    protected Kryo propertiesKryo;
    protected Kryo kryo;

    public String fileInputFormat;
    public String expectedFileInputFormat;

    public String serializationLib;
    public String expectedSerializationLib;

    public PropertiesSerializerTest(String fileInputFormat, String expectedFileInputFormat,
                                    String serializationLib, String expectedSerializationLib) {
        this.fileInputFormat = fileInputFormat;
        this.expectedFileInputFormat = expectedFileInputFormat;
        this.serializationLib = serializationLib;
        this.expectedSerializationLib = expectedSerializationLib;
    }

    @Before
    public void setup() {
        propertiesKryo = new Kryo();
        propertiesKryo.addDefaultSerializer(Map.class, PropertiesSerializer.class);

        kryo = new Kryo();
    }

    @Test
    public void testFileInputFormatSerialization() {
        Properties properties = new Properties();
        properties.put("file.inputformat", fileInputFormat);

        Output out = new Output(4 * 1024, 10 * 1024 * 1024);
        propertiesKryo.writeObject(out, properties);
        out.close();

        byte[] bytes = out.toBytes();
        Properties propertiesResult = kryo.readObject(new Input(bytes), Properties.class);
        assertEquals(expectedFileInputFormat, propertiesResult.get("file.inputformat"));
    }

    @Test
    public void testSerializationLibSerialization() {
        Properties properties = new Properties();
        properties.put("serialization.lib", serializationLib);

        Output out = new Output(4 * 1024, 10 * 1024 * 1024);
        propertiesKryo.writeObject(out, properties);
        out.close();

        byte[] bytes = out.toBytes();
        Properties propertiesResult = kryo.readObject(new Input(bytes), Properties.class);
        assertEquals(expectedSerializationLib, propertiesResult.get("serialization.lib"));
    }

    @Test
    public void testFileInputFormatDeserialization() {
        Properties properties = new Properties();
        properties.put("file.inputformat", expectedFileInputFormat);

        Output out = new Output(4 * 1024, 10 * 1024 * 1024);
        propertiesKryo.writeObject(out, properties);
        out.close();

        byte[] bytes = out.toBytes();
        Properties propertiesResult = propertiesKryo.readObject(new Input(bytes), Properties.class);
        assertEquals(fileInputFormat, propertiesResult.get("file.inputformat"));
    }

    @Test
    public void testSerializationLibDeserialization() {
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