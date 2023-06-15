package org.greenplum.pxf.api.function;

import java.util.Objects;
import java.util.function.Function;

/**
 * Represents a function that accepts four arguments and produces a result.
 * This is the four-arity specialization of {@link Function}.
 * This is a functional interface whose functional method is {@link #apply(Object, Object, Object, Object)}.
 *
 * @param <T> The first argument type
 * @param <U> The second argument type
 * @param <V> The third argument type
 * @param <S> The fourth argument type
 * @param <R> The result type
 * @see Function
 */
public interface QuadriFunction<T, U, V, S, R> {
    /**
     * Applies this function to the given arguments.
     *
     * @param t the first function argument
     * @param u the second function argument
     * @param v the third function argument
     * @param s the fourth function argument
     * @return The result
     */
    R apply(T t, U u, V v, S s);

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
    default <W> QuadriFunction<T, U, V, S, W> andThen(
            Function<? super R, ? extends W> after) {
        Objects.requireNonNull(after);
        return (T t, U u, V v, S s) -> after.apply(apply(t, u, v, s));
    }
}
