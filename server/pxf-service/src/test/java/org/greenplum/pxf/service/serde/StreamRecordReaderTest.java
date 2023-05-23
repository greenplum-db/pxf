package org.greenplum.pxf.service.serde;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.DataInputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
public class StreamRecordReaderTest {
    @Mock
    private DataInputStream mockInputStream;

    @Test
    public void testReadRecord() throws Exception {
        StreamRecordReader reader = new StreamRecordReader(new RequestContext());
        List<OneField> record = reader.readRecord(mockInputStream);
        assertNotNull(record);
        assertEquals(1, record.size());
        assertEquals(DataType.BYTEA.getOID(), record.get(0).type);
        assertSame(mockInputStream, record.get(0).val);
        verifyNoInteractions(mockInputStream); // no reading should actually happen
    }
}
