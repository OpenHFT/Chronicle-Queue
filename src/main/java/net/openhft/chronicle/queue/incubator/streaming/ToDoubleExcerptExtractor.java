package net.openhft.chronicle.queue.incubator.streaming;

import net.openhft.chronicle.core.annotation.NonNegative;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;

import static java.lang.Double.isNaN;
import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public interface ToDoubleExcerptExtractor {

    /**
     * Extracts a value of type {@code double } from the provided {@code wire} and {@code index} or else {@link Double#NaN}
     * if no value can be extracted.
     * <p>
     * {@link Double#NaN} may be returned if the queue was written with a method writer and there are messages in the
     * queue but of another type.
     * <p>
     * Extractors that must include {@link Double#NaN} as a valid value must use other means of
     * aggregating values (e.g. use an {@link ExcerptExtractor ExcerptExtractor<Double> }.
     *
     * @param wire  to use
     * @param index to use
     * @return extracted value or {@link Double#NaN}
     */
    double extractAsDouble(@NotNull Wire wire, @NonNegative long index);

    /**
     * Creates and returns a new ToDoubleExcerptExtractor consisting of the results (of type R) of applying the provided
     * {@code mapper } to the elements of this ToDoubleExcerptExtractor.
     * <p>
     * Values mapped to {@link Double#NaN} are removed.
     *
     * @param mapper to apply
     * @return a new mapped ToDoubleExcerptExtractor
     * @throws NullPointerException if the provided {@code mapper} is {@code null}
     */
    default ToDoubleExcerptExtractor map(@NotNull final DoubleUnaryOperator mapper) {
        requireNonNull(mapper);
        return (wire, index) -> {
            final double value = extractAsDouble(wire, index);
            if (isNaN(value)) {
                return Double.NaN;
            }
            return mapper.applyAsDouble(value);
        };
    }

    /**
     * Creates and returns a new ExcerptExtractor consisting of applying the provided
     * {@code mapper } to the elements of this ExcerptExtractor.
     * <p>
     * Values mapped to {@link Double#NaN } are removed.
     *
     * @param mapper to apply
     * @return a new mapped ExcerptExtractor
     * @throws NullPointerException if the provided {@code mapper} is {@code null}
     */
    default <T> ExcerptExtractor<T> mapToObj(@NotNull final DoubleFunction<? extends T> mapper) {
        requireNonNull(mapper);
        return (wire, index) -> {
            final double value = extractAsDouble(wire, index);
            if (isNaN(value)) {
                return null;
            }
            return mapper.apply(value);
        };
    }

    /**
     * Creates and returns a new ToLongExcerptExtractor consisting of applying the provided
     * {@code mapper } to the elements of this ToDoubleExcerptExtractor.
     * <p>
     * Values mapped to {@link Double#NaN } are removed.
     *
     * @param mapper to apply
     * @return a new mapped ToLongExcerptExtractor
     * @throws NullPointerException if the provided {@code mapper} is {@code null}
     */
    default ToLongExcerptExtractor mapToLong(@NotNull final DoubleToLongFunction mapper) {
        requireNonNull(mapper);
        return (wire, index) -> {
            final double value = extractAsDouble(wire, index);
            if (isNaN(value)) {
                return Long.MIN_VALUE;
            }
            return mapper.applyAsLong(value);
        };
    }

    /**
     * Creates and returns a new ToDoubleExcerptExtractor consisting of the elements of this ToDoubleExcerptExtractor
     * that match the provided {@code predicate}.
     *
     * @param predicate to apply to each element to determine if it
     *                  should be included
     * @return a ToDoubleExcerptExtractor consisting of the elements of this ToDoubleExcerptExtractor that match
     * @throws NullPointerException if the provided {@code predicate} is {@code null}
     */
    default ToDoubleExcerptExtractor filter(@NotNull final DoublePredicate predicate) {
        requireNonNull(predicate);
        return (wire, index) -> {
            final double value = extractAsDouble(wire, index);
            if (isNaN(value)) {
                // The value is already filtered so just propagate the lack of a value
                return Double.NaN;
            }
            return predicate.test(value)
                    ? value
                    : Double.NaN;
        };
    }

    // skip

    // peek

}