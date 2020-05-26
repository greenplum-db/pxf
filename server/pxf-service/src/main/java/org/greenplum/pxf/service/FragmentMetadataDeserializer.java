package org.greenplum.pxf.service;

import org.greenplum.pxf.api.utilities.FragmentMetadata;

/**
 * Serializes and deserializes partition metadata
 */
public interface FragmentMetadataDeserializer {

    /**
     * Returns the deserialized {@link FragmentMetadata} object
     *
     * @param serializedFragmentMetadata the String representation of the {@link FragmentMetadata} object
     * @return the deserialized {@link FragmentMetadata} object
     */
    FragmentMetadata deserialize(String serializedFragmentMetadata);
}
