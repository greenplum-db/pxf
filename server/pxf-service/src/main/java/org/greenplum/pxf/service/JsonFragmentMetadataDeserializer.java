package org.greenplum.pxf.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.greenplum.pxf.api.utilities.FragmentMetadata;
import org.springframework.stereotype.Service;

import java.text.DateFormat;

/**
 * Serializes {@link FragmentMetadata} objects to a Json representation of
 * the object, and deserializes the serialized byte[] representation back into
 * the {@link FragmentMetadata} object
 */
@Service
public class JsonFragmentMetadataDeserializer implements FragmentMetadataDeserializer {

    private final Gson gson;

    public JsonFragmentMetadataDeserializer() {
        GsonBuilder builder = new GsonBuilder();
        builder.setDateFormat("yyyy-MM-dd");
        builder.registerTypeAdapter(FragmentMetadata.class, new JsonInterfaceAdapter<>());
        gson = builder.create();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FragmentMetadata deserialize(String json) {
        return gson.fromJson(json, FragmentMetadata.class);
    }
}
