package org.greenplum.pxf.api;

import org.greenplum.pxf.api.io.DataType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ArrayFieldTest {
    @Test
    public void testSimpleTypes() {
        ArrayField field = new ArrayField(DataType.INT8ARRAY.getOID(), new ArrayList<Integer>(){{ add(0); add(1); }});
        Assert.assertEquals("{0,1}",field.toString());
        field = new ArrayField(DataType.TEXTARRAY.getOID(), new ArrayList<String>(){{ add("foo"); add("bar"); }});
        Assert.assertEquals("{foo,bar}",field.toString());
    }

    @Test
    public void testComplexTypes() {
        List<List<Integer>> nestedList = new ArrayList<>();
        nestedList.add(new ArrayList<Integer>() {{
            add(0);
            add(1);
        }});
        nestedList.add(new ArrayList<Integer>() {{
            add(1);
            add(0);
        }});
        ArrayField field = new ArrayField(DataType.INT8ARRAY.getOID(), nestedList);
        Assert.assertEquals("{{0,1},{1,0}}",field.toString());
    }

}