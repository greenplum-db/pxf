package org.greenplum.pxf.api;

/**
 * Interface a user of FilterParser should implement.
 * This is used to let the user build filter expressions in the manner they see fit.
 * When an operator is parsed, this function is called to let the user decide what to do with its operands.
 */
public interface FilterBuilder {
    /**
     * Builds the filter for an operator with 2 operands
     *
     * @param operator the parsed operator to perform
     * @param left      the left operand
     * @param right     the right operand
     * @return the built filter
     * @throws Exception if building the filter failed
     */
    Object build(FilterParser.Operator operator, Object left, Object right) throws Exception;

    /**
     * Builds the filter for an operator with one operand
     *
     * @param operator the parsed operator to perform
     * @param operand   the single operand
     * @return the built filter
     * @throws Exception if building the filter failed
     */
    Object build(FilterParser.Operator operator, Object operand) throws Exception;
}
