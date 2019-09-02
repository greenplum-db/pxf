package org.greenplum.pxf.api;

/**
 * Interface a user of FilterParser should implement.
 * This is used to let the user build filter expressions in the manner she sees fit.
 * When an operator is parsed, this function is called to let the user decide what to do with its operands.
 */
public interface FilterBuilder {
    /**
     * Builds the filter for an operation with 2 operands
     *
     * @param operation the parsed operation to perform
     * @param left      the left operand
     * @param right     the right operand
     * @return the built filter
     * @throws Exception if building the filter failed
     */
    Object build(FilterParser.Operation operation, Object left, Object right) throws Exception;

    /**
     * Builds the filter for an operation with one operand
     *
     * @param operation the parsed operation to perform
     * @param operand   the single operand
     * @return the built filter
     * @throws Exception if building the filter failed
     */
    Object build(FilterParser.Operation operation, Object operand) throws Exception;

    /**
     * Builds the filter for a logical operation and two operands
     *
     * @param operation the parsed logical operation to perform
     * @param left      the left operand
     * @param right     the right operand
     * @return the built filter
     * @throws Exception if building the filter failed
     */
    Object build(FilterParser.LogicalOperation operation, Object left, Object right) throws Exception;

    /**
     * Builds the filter for a logical operation and one operand
     *
     * @param operation the parsed unary logical operation to perform
     * @param filter    the single operand
     * @return the built filter
     * @throws Exception if building the filter failed
     */
    Object build(FilterParser.LogicalOperation operation, Object filter) throws Exception;
}
