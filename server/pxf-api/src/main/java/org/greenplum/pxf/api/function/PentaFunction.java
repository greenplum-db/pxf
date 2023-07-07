package org.greenplum.pxf.api.function;

import java.util.Objects;
import java.util.function.Function;

/**
 * Represents a function that accepts five arguments and produces a result.
 * This is the five-arity specialization of {@link Function}.
 * This is a functional interface whose functional method is {@link #apply(Object, Object, Object, Object, Object)}.
 *
 * @param <P> The first argument type
 * @param <S> The second argument type
 * @param <T> The third argument type
 * @param <U> The fourth argument type
 * @param <V> The fifth argument type
 * @param <W> The result type
 * @see Function
 */
public interface PentaFunction<P, S, T, U, V, W> {
    /**
     * Applies this function to the given arguments.
     *
     * @param p the first function argument
     * @param s the second function argument
     * @param t the third function argument
     * @param u the fourth function argument
     * @param v the fifth function argument
     * @return The result
     */
    W apply(P p, S s, T t, U u, V v);

    /**
     * Returns a composed function that first applies this function to
     * its input, and then applies the {@code after} function to the result.
     * If evaluation of either function throws an exception, it is relayed to
     * the caller of the composed function.
     *
     * @param <X>   the type of output of the {@code after} function, and of the
     *              composed function
     * @param after the function to apply after this function is applied
     * @return a composed function that first applies this function and then
     * applies the {@code after} function
     * @throws NullPointerException if after is null
     */
    default <X> PentaFunction<P, S, T, U, V, X> andThen(
            Function<? super W, ? extends X> after) {
        Objects.requireNonNull(after);
        return (P p, S s, T t, U u, V v) -> after.apply(apply(p, s, t, u, v));
    }
}
