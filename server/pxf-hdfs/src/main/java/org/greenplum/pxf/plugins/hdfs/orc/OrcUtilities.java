package org.greenplum.pxf.plugins.hdfs.orc;

import org.apache.commons.lang.StringUtils;
import org.apache.orc.TypeDescription;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

@Component
public final class OrcUtilities {

    private static final Logger LOG = LoggerFactory.getLogger(OrcUtilities.class);

    private PgUtilities pgUtilities;

    /**
     * default constructor
     */
    public OrcUtilities(PgUtilities pgUtilities) {
        this.pgUtilities = pgUtilities;
    }

    /**
     * Parse a Postgres external format into a given ORC schema
     *
     * Re-used from GPHDFS
     * https://github.com/greenplum-db/gpdb/blob/3b0bfdc169fab7f686276be7eccb024a5e29543c/gpAux/extensions/gphdfs/src/java/1.2/com/emc/greenplum/gpdb/hadoop/formathandler/util/FormatHandlerUtil.java
     * @param val Postgres external format (the output of function named by typoutput in pg_type) or `null` if null value
     * @param type ORC list type
     * @return
     */

    public List<Object> parsePostgresArray (String val, TypeDescription type) {
        LOG.debug("type={}, value={}, isTopLevel={}", type, val);
        TypeDescription underlyingType = type.getChildren().get(0);

        if (val == null) {
            return null;
        }

        if (type.getCategory() != TypeDescription.Category.LIST) {
            throw new PxfRuntimeException(String.format("Value %s was not of expected type %s", val, type.getCategory()));
        }

        List<Object> data = new ArrayList<>();
        if (!val.equalsIgnoreCase("{}")) {
            String[] splits = pgUtilities.splitArray(val);
            for (String split : splits) {
                try {
                    data.add(decodeString(split, underlyingType.getCategory()));
                } catch (NumberFormatException | PxfRuntimeException e) {
                    String hint = "";
                    if (StringUtils.startsWith(split, "{")) {
                        hint = "Value is a multi-dimensional array, PXF does not currently support multi-dimensional arrays for writing ORC files.";
                    } else {
                        hint = "Unexpected state since PXF generated the ORC schema.";
                    }
                    throw new PxfRuntimeException(String.format("Error parsing array element: %s was not of expected type %s", split, underlyingType.getCategory()), hint, e);
                }
            }
        }
        return data;
    }

    private Object decodeString(String val, TypeDescription.Category valType) {
        if (val == null) {
            return null;
        }

        switch (valType) {
            case BOOLEAN:
                return pgUtilities.parseBoolLiteral(val);
            case BYTE:
                return val.getBytes(Charset.defaultCharset());
            case SHORT:
            case INT:
            case LONG:
                return Long.parseLong(val);
            case FLOAT:
                return Float.parseFloat(val);
            case DOUBLE:
                return Double.parseDouble(val);
            case BINARY:
                return pgUtilities.parseByteaLiteral(val).array();
            case STRING:
            case CHAR:
            case VARCHAR:
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_INSTANT:
            case DECIMAL:
                return val;
            default:
                throw new PxfRuntimeException(String.format("type: %s is not supported", valType));
        }
    }
}
