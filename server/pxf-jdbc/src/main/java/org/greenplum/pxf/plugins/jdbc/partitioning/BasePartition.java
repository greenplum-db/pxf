package org.greenplum.pxf.plugins.jdbc.partitioning;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * A base class for partition of any type.
 * <p>
 * All partitions use some column as a partition column. It is processed by this class.
 */
@RequiredArgsConstructor
public abstract class BasePartition implements JdbcFragmentMetadata {

    /**
     * Column name to use as a partition column. Must not be null
     */
    @Getter
    @NonNull
    protected final String column;

    /**
     * Generate a range-based SQL constraint
     *
     * @param quotedColumn column name (used as is, thus it should be quoted if necessary)
     * @param range        range to base constraint on
     * @return a pure SQL constraint (without WHERE)
     */
    String generateRangeConstraint(String quotedColumn, String[] range) {
        StringBuilder sb = new StringBuilder(quotedColumn);

        if (range.length == 1) {
            sb.append(" = ").append(range[0]);
        } else if (range[0] == null) {
            sb.append(" < ").append(range[1]);
        } else if (range[1] == null) {
            sb.append(" >= ").append(range[0]);
        } else {
            sb.append(" >= ").append(range[0])
                    .append(" AND ")
                    .append(quotedColumn).append(" < ").append(range[1]);
        }

        return sb.toString();
    }
}
