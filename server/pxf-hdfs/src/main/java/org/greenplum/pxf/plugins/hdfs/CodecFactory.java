package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CodecFactory {

    private static final CodecFactory codecFactoryInstance = new CodecFactory();

    /**
     * Returns the {@link CompressionCodecName} for the given name, or default if name is null
     *
     * @param name         the name or class name of the compression codec
     * @param defaultCodec the default codec
     * @return the {@link CompressionCodecName} for the given name, or default if name is null
     */
    public CompressionCodecName getCodec(String name, CompressionCodecName defaultCodec) {
        if (name == null) return defaultCodec;

        try {
            return CompressionCodecName.fromConf(name);
        } catch (IllegalArgumentException ie) {
            try {
                return CompressionCodecName.fromCompressionCodec(Class.forName(name));
            } catch (ClassNotFoundException ce) {
                throw new IllegalArgumentException(String.format("Invalid codec: %s ", name));
            }
        }
    }

    /**
     * Helper routine to get compression codec through reflection.
     *
     * @param name codec name
     * @param conf configuration used for reflection
     * @return generated CompressionCodec
     */
    public CompressionCodec getCodec(String name, Configuration conf) {
        return ReflectionUtils.newInstance(getCodecClass(name, conf), conf);
    }

    /*
     * Helper routine to get a compression codec class
     */
    public Class<? extends CompressionCodec> getCodecClass(String name, Configuration conf) {
        Class<? extends CompressionCodec> codecClass;
        try {
            codecClass = conf.getClassByName(name).asSubclass(CompressionCodec.class);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    String.format("Compression codec %s was not found.", name), e);
        }
        return codecClass;
    }

    /**
     * Returns a singleton instance of the codec factory.
     *
     * @return a singleton instance of the codec factory
     */
    public static CodecFactory getInstance() {
        return codecFactoryInstance;
    }
}
