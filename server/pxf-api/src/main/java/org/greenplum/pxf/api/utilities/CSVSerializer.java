package org.greenplum.pxf.api.utilities;

import org.apache.commons.codec.binary.Hex;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.io.DataType;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class CSVSerializer {
    public static final char QUOTE = '"';
    public static final String VALUE_OF_NULL = "";

    public String fieldListToCSVString(List<OneField> fields) {
        return fields.stream()
                .map(s -> {
                    if (s.val == null)
                        return VALUE_OF_NULL;
                    else if (s.type == DataType.BYTEA.getOID())
                        return "\\x" + Hex.encodeHexString((byte[]) s.val);
                    else if (s.type == DataType.NUMERIC.getOID() || !DataType.isTextForm(s.type))
                        return Objects.toString(s.val, null);
                    else
                        return Utilities.toCsvText((String) s.val, QUOTE, true, true);
                })
                .collect(Collectors.joining(","));
    }
}
