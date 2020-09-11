package org.greenplum.pxf.api.utilities;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * This class serializes and deserializes {@link FragmentMetadata} objects into
 * JSON.
 */
@Component
public class FragmentMetadataSerDe extends StdSerializer<FragmentMetadata> {

    private static final long serialVersionUID = 123173996615107417L;
    private static final String CLASSNAME = "className";

    private final ObjectMapper mapper;

    /**
     * Private constructor to prevent initialization
     */
    public FragmentMetadataSerDe() {
        super(FragmentMetadata.class);
        mapper = new ObjectMapper();
    }

    @Override
    public void serialize(FragmentMetadata value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeString(mapper.writeValueAsString(value));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public FragmentMetadata deserialize(String json) throws JsonProcessingException {
        JsonNode node = mapper.readTree(json);
        String className = node.get(CLASSNAME).textValue();

        Class klass = getObjectClass(className);
        return (FragmentMetadata) mapper.readValue(json, klass);
    }

    @SuppressWarnings("rawtypes")
    private Class getObjectClass(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
