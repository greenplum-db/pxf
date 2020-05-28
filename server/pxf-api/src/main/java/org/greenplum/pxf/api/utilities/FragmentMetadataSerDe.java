package org.greenplum.pxf.api.utilities;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.google.gson.JsonParseException;

import java.io.IOException;

/**
 * This class serializes and deserializes {@link FragmentMetadata} objects into
 * JSON.
 */
public class FragmentMetadataSerDe extends StdSerializer<FragmentMetadata> {

    private static final long serialVersionUID = 123173996615107417L;
    private static final String CLASSNAME = "className";

    /**
     * Singleton instance of the FragmentMetadataSerDe
     */
    private static final FragmentMetadataSerDe INSTANCE = new FragmentMetadataSerDe();

    public final ObjectMapper mapper;

    /**
     * Private constructor to prevent initialization
     */
    private FragmentMetadataSerDe() {
        super(FragmentMetadata.class);
        mapper = new ObjectMapper();
    }

    /**
     * Returns the singleton instance of this class
     *
     * @return the singleton instance of this class
     */
    public static FragmentMetadataSerDe getInstance() {
        return INSTANCE;
    }

    @Override
    public void serialize(FragmentMetadata value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeString(mapper.writeValueAsString(value));
    }

    @SuppressWarnings("unchecked")
    public FragmentMetadata deserialize(String json) throws JsonProcessingException {
        JsonNode node = mapper.readTree(json);
        String className = node.get(CLASSNAME).textValue();

        Class klass = getObjectClass(className);
        return (FragmentMetadata) mapper.readValue(json, klass);
    }

    private Class getObjectClass(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new JsonParseException(e.getMessage());
        }
    }
}
