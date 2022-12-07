package org.greenplum.pxf.plugins.hdfs.utilities;

import java.util.function.Consumer;

/**
 * Utility methods for converting between Java types and Postgres types text format
 */
public class PgArrayBuilder {

    private final StringBuilder buffer;
    private final PgUtilities pgUtilities;
    private boolean isFirst = true;

    public PgArrayBuilder(PgUtilities pgUtilities) {
        buffer = new StringBuilder();
        this.pgUtilities = pgUtilities;
    }

    public void startArray() {
        buffer.append('{');
    }

    /**
     * Add the given element to the list array and handle any string escaping
     * @param elem  the element to be escaped and added to the array
     */
    public void addElement(String elem) {
        addDelim();
        buffer.append(pgUtilities.escapeArrayElement(elem));
    }

    /**
     * Add the given element to the list array and handle any string escaping
     *
     * @param elem the element to be added to the array
     * @param needsEscapingInArray whether the element need to be escaped before being added to the array
     */
    public void addElement(String elem, boolean needsEscapingInArray) {
        if (needsEscapingInArray) {
            addElement(elem);
        } else {
            addElementNoEscaping(elem);
        }
    }

    /**
     * Add the given element to the list array.
     * This function assumes that the element has
     * either already been escaped, or does not need escaping.
     * @param elem the element to be added to the array
     */
    public void addElementNoEscaping(String elem) {
        addDelim();
        buffer.append(elem);
    }

    /**
     * Add the element provided by the consumer to the array
     *
     * This is an optimization to avoid allocating new Strings. Instead, we
     * allow callers to act directly on the buffer through this function.
     * @param consumer the consumer function that modifies the buffer itself
     */
    public void addElement(Consumer<StringBuilder> consumer) {
        addDelim();
        consumer.accept(buffer);
    }

    /**
     * Adds the array delimiter only if it is not the first element in the array
     */
    public void addDelim() {
        if (!isFirst) {
            buffer.append(',');
        } else {
            isFirst = false;
        }
    }

    public void endArray() {
        buffer.append('}');
    }

    public String toString() {
        return buffer.toString();
    }

}
