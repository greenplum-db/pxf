package org.greenplum.pxf.api.function;

import java.util.Objects;
import java.util.function.Function;

/**
 * Represents a function that accepts five arguments and produces a result.
 * This is the five-arity specialization of {@link Function}.
 * This is a functional interface whose functional method is {@link #apply(Object, Object, Object, Object, Object)}.
 *
 * @param <T> The first argument type
 * @param <U> The second argument type
 * @param <V> The third argument type
 * @param <S> The fourth argument type
 * @param <P> The fifth argument type
 * @param <R> The result type
 * @see Function
 */
public interface PentaFunction<T, U, V, S, P, R> {
    /**
     * Applies this function to the given arguments.
     *
     * @param t the first function argument
     * @param u the second function argument
     * @param v the third function argument
     * @param s the fourth function argument
     * @param p the fifth input argument
     * @return The result
     */
    R apply(T t, U u, V v, S s, P p);

    /**
     * Returns a composed function that first applies this function to
     * its input, and then applies the {@code after} function to the result.
     * If evaluation of either function throws an exception, it is relayed to
     * the caller of the composed function.
     *
     * @param <W>   the type of output of the {@code after} function, and of the
     *              composed function
     * @param after the function to apply after this function is applied
     * @return a composed function that first applies this function and then
     * applies the {@code after} function
     * @throws NullPointerException if after is null
     */
    default <W> PentaFunction<T, U, V, S, P, W> andThen(
            Function<? super R, ? extends W> after) {
        Objects.requireNonNull(after);
        return (T t, U u, V v, S s, P p) -> after.apply(apply(t, u, v, s, p));
    }
}
