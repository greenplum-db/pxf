package org.greenplum.pxf.api.model;

import java.io.IOException;

/**
 * Interface that adds Iterable behavior to a Resolver for
 * the purpose of streaming large chunks of data. A large
 * data field can be a StreamingField (scalar or ArrayStreamingField)
 *
 * Note that the resolver can return an arbitrary list of OneFields,
 * but there should only be one streaming field.
 */
public interface StreamingResolver extends Resolver {
    /**
     * @return the next piece of data
     */
    Object next() throws IOException, InterruptedException;

    /**
     * @return whether or not the streaming field has any more data
     */
    boolean hasNext();
}
