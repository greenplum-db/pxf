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

    @Test
    public void testNeedsEscapingInArray() {
        for (DataType dataType : DataType.values()) {
            if (dataType.isArrayType()) {
                if (needsEscapingForElementsInArray(dataType.getOID())) {
                    assertTrue(dataType.getTypeElem().getNeedsEscapingInArray());
                } else {
                    assertFalse(dataType.getTypeElem().getNeedsEscapingInArray());
                }
                assertTrue(dataType.getNeedsEscapingInArray());
            } else if (dataType.getOID() == DataType.UNSUPPORTED_TYPE.getOID()) {
                assertTrue(dataType.getNeedsEscapingInArray());
            }
        }
    }

    private boolean needsEscapingForElementsInArray(int oid) {
        return oid == DataType.BYTEAARRAY.getOID() || oid == DataType.TEXTARRAY.getOID() ||
                oid == DataType.BPCHARARRAY.getOID() || oid == DataType.VARCHARARRAY.getOID() ||
                oid == DataType.TIMEARRAY.getOID() || oid == DataType.TIMESTAMPARRAY.getOID() ||
                oid == DataType.TIMESTAMP_WITH_TIMEZONE_ARRAY.getOID();
    }

}
