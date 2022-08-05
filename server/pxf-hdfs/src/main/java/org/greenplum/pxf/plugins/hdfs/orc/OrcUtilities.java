package org.greenplum.pxf.plugins.hdfs.orc;

import org.apache.commons.lang.StringUtils;
import org.apache.orc.TypeDescription;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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
     * @param underlyingChildCategory Underlying ORC type for ORC list. This functions assumes the ORC list is one-dimensional
     * @return
     */
    public List<Object> parsePostgresArray (String val, TypeDescription.Category underlyingChildCategory) {
        LOG.debug("child type={}, value={}", underlyingChildCategory, val);

        if (val == null) {
            return null;
        }

        String[] splits = pgUtilities.splitArray(val);
        List<Object> data = new ArrayList<>(splits.length);
        for (String split : splits) {
            try {
                data.add(decodeString(split, underlyingChildCategory));
            } catch (NumberFormatException | PxfRuntimeException e) {
                String hint = createErrorHintFromValue(split);
                throw new PxfRuntimeException(String.format("Error parsing array element: %s was not of expected type %s", split, underlyingChildCategory), hint, e);
            }
        }
        return data;
    }

    /**
     * Parse a string based off the ORC type.
     *
     * @param val String representation of value
     * @param orcType ORC type
     * @return a java representation of the value for the given valType
     */
    private Object decodeString(String val, TypeDescription.Category orcType) {
        if (val == null) {
            return null;
        }

        switch (orcType) {
            // ignore BYTE case -- for now ORCSchemaBuilder does not support this type, so we do not expect it
            case BOOLEAN:
                return pgUtilities.parseBoolLiteral(val);
            case SHORT:
            case INT:
            case LONG:
                return Long.parseLong(val);
            case FLOAT:
                return Float.parseFloat(val);
            case DOUBLE:
                return Double.parseDouble(val);
            case BINARY:
                return pgUtilities.parseByteaLiteral(val);
            case STRING:
            case CHAR:
            case VARCHAR:
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_INSTANT:
            case DECIMAL:
                return val;
            default:
                throw new PxfRuntimeException(String.format("type: %s is not supported", orcType));
        }
    }

    /**
     * Determine the error hint to return based off the value of the column
     * @param val the array value that caused the error
     * @return the appropriate error hint
     */
    protected String createErrorHintFromValue(String val) {
        if (StringUtils.startsWith(val, "{")) {
            return "Value is a multi-dimensional array, PXF does not support multi-dimensional arrays for writing ORC files.";
        } else {
            return "Unexpected state since PXF generated the ORC schema.";
        }
    }
}
