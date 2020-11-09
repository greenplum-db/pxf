package org.greenplum.pxf.plugins.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.apache.hadoop.mapred.FileSplit;
import org.greenplum.pxf.plugins.hdfs.HcfsFragmentMetadata;

/**
 * Fragment Metadata for Hive
 */
@Getter
public class HiveFragmentMetadata extends HcfsFragmentMetadata {

    /**
     * Kryo-serialized properties needed for SerDe initialization
     */
    private final byte[] kryoProperties;

    /**
     * Default constructor for JSON serialization
     */
    @JsonCreator
    public HiveFragmentMetadata(
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("kryoProperties") byte[] kryoProperties) {
        super(start, length);
        this.kryoProperties = kryoProperties;
    }

    /**
     * Constructs a {@link HiveFragmentMetadata} object with the given
     * {@code fileSplit} and the {@code kryoProperties}.
     *
     * @param fileSplit      the {@link FileSplit} object.
     * @param kryoProperties the properties that have been serialized to kryo
     */
    public HiveFragmentMetadata(FileSplit fileSplit, byte[] kryoProperties) {
        super(fileSplit);
        this.kryoProperties = kryoProperties;
    }
}
