package org.greenplum.pxf.api.io;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataTypeTest {
    @Test
    public void enumElementArrayMapping() {
        for (DataType dataType : DataType.values()) {
            if (dataType.equals(DataType.UNSUPPORTED_TYPE)) {
                assertNull(dataType.getTypeElem());
                assertNull(dataType.getTypeArray());
                assertFalse(dataType.isArrayType());
            } else if (dataType.getTypeElem() != null) {
                // an array type
                assertNull(dataType.getTypeArray());
                assertTrue(dataType.isArrayType());
                DataType elementType = dataType.getTypeElem();
                assertEquals(dataType, elementType.getTypeArray());
            } else {
                // a primitive type
                assertNotNull(dataType.getTypeArray());
                assertFalse(dataType.isArrayType());
                DataType arrayType = dataType.getTypeArray();
                assertEquals(dataType, arrayType.getTypeElem());
            }
        }
    }
}
