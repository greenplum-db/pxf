package org.greenplum.pxf.api;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A special case of OneField that represents an array.
 */
public class ArrayField extends OneField {
    private String separator = ",";

    /**
     * @param type Should be one of the DataType.*ARRAY types in the DataType class
     * @param val  A list of values that comprise the array
     */
    public ArrayField(int type, List<?> val) {
        super(type, val);
        this.setPrefix("{");
        this.setSuffix("}");
    }

    public String getSeparator() {
        return this.separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    @Override
    public String toString() {
        List<?> list = (List<?>) this.val;
        // in case we have nested arrays
        // String result = nested.stream().map(
        //       nextList -> nextList.stream()
        //         .collect(Collectors.joining("-")))
        //       .collect(Collectors.joining("; "));
        return list.toString()
                .replaceAll("\\[", prefix)
                .replaceAll("]", suffix)
                .replaceAll(",", separator)
                .replaceAll(" ", "");
    }
}
