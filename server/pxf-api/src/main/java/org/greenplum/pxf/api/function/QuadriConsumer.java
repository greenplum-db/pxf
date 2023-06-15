package org.greenplum.pxf.api.function;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Represents an operation that accepts four input arguments and returns no
 * result.  This is the four-arity specialization of {@link Consumer}.
 * Unlike most other functional interfaces, {@code TriConsumer} is expected
 * to operate via side-effects.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #accept(Object, Object, Object, Object)}.
 *
 * @param <T> the type of the first argument to the operation
 * @param <U> the type of the second argument to the operation
 * @param <V> the type of the third argument to the operation
 * @param <S> the type of the fourth argument to the operation
 *
 * @see Consumer
 */
@FunctionalInterface
public interface QuadriConsumer<T, U, V, S> {
    /**
     * Performs this operation on the given arguments.
     *
     * @param t the first input argument
     * @param u the second input argument
     * @param v the third input argument
     * @param s the fourth input argument
     */
    void accept(T t, U u, V v, S s);

    /**
     * Returns a composed {@code QuadriConsumer} that performs, in sequence, this
     * operation followed by the {@code after} operation. If performing either
     * operation throws an exception, it is relayed to the caller of the
     * composed operation.  If performing this operation throws an exception,
     * the {@code after} operation will not be performed.
     *
     * @param after the operation to perform after this operation
     * @return a composed {@code QuadriConsumer} that performs in sequence this
     * operation followed by the {@code after} operation
     * @throws NullPointerException if {@code after} is null
     */
    default QuadriConsumer<T, U, V, S> andThen(QuadriConsumer<? super T, ? super U, ? super V, ? super S> after) {
        Objects.requireNonNull(after);
        return (t, u, v, s) -> {
            accept(t, u, v, s);
            after.accept(t, u, v, s);
        };
    }
}
