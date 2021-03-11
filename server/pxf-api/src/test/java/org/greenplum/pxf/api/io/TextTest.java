package org.greenplum.pxf.api.io;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.DataOutput;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class TextTest {

    @Mock
    private DataOutput mockOutput;

    @Test
    public void testWriteBytes() throws IOException {
        Text text = new Text("hello");
        long bytes = text.write(mockOutput);
        assertEquals(5, bytes);
        verify(mockOutput).write(text.getBytes(), 0, 5);
    }
}
