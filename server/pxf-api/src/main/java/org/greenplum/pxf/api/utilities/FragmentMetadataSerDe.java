package org.greenplum.pxf.api.utilities;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.commons.codec.binary.Base64;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * This class serializes and deserializes {@link FragmentMetadata} objects.
 */
@Component
public class FragmentMetadataSerDe extends StdSerializer<FragmentMetadata> {

    private static final long serialVersionUID = 123173996615107417L;
    private final SerializationService serializationService;

    /**
     * Private constructor to prevent initialization
     */
    public FragmentMetadataSerDe(SerializationService serializationService) {
        super(FragmentMetadata.class);
        this.serializationService = serializationService;
    }

    @Override
    public void serialize(FragmentMetadata value, JsonGenerator gen, SerializerProvider provider)
            throws IOException {
        Output out = new Output(4 * 1024, 10 * 1024 * 1024);
        Kryo kryo = serializationService.borrowKryo();
        try {
            kryo.writeClassAndObject(out, value);
            out.close();
            // Serialized fragment metadata is base64 encoded
            gen.writeBinary(out.toBytes());
        } finally {
            serializationService.releaseKryo(kryo);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends FragmentMetadata> T deserialize(String metadata) {
        Kryo kryo = serializationService.borrowKryo();
        try {
            byte[] decoded = Base64.decodeBase64(metadata);
            return (T) kryo.readClassAndObject(new Input(decoded));
        } finally {
            serializationService.releaseKryo(kryo);
        }
    }
}
