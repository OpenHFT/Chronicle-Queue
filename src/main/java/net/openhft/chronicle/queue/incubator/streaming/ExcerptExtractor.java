package net.openhft.chronicle.queue.incubator.streaming;

import net.openhft.chronicle.core.annotation.NonNegative;
import net.openhft.chronicle.queue.internal.streaming.AccumulatorUtil;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

@FunctionalInterface
public interface ExcerptExtractor<T> {

    /**
     * Extracts a value of type T from the provided {@code wire} and {@code index} or else {@code null}
     * if no value can be extracted.
     * <p>
     * {@code null} may be returned if the queue was written with a method writer and there are messages in the
     * queue but of another type.
     *
     * @param wire  to use
     * @param index to use
     * @return extracted value or {@code null}
     */
    @Nullable
    T extract(@NotNull Wire wire, @NonNegative long index);


    /**
     * Creates and returns a new ExcerptExtractor consisting of the results (of type R) of applying the provided
     * {@code mapper } to the elements of this ExcerptExtractor.
     * <p>
     * Values mapped to {@code null} are removed.
     *
     * @param mapper to apply
     * @param <R>    type to map to
     * @return a new mapped ExcerptExtractor
     * @throws NullPointerException if the provided {@code mapper} is {@code null}
     */
    default <R> ExcerptExtractor<R> map(@NotNull final Function<? super T, ? extends R> mapper) {
        requireNonNull(mapper);
        return (wire, index) -> {
            final T value = extract(wire, index);
            if (value == null) {
                return null;
            }
            return mapper.apply(value);
        };
    }

    /**
     * Creates and returns a new ToLongExcerptExtractor consisting of applying the provided
     * {@code mapper } to the elements of this ExcerptExtractor.
     * <p>
     * Values mapped to {@link Long#MIN_VALUE } are removed.
     *
     * @param mapper to apply
     * @return a new mapped ExcerptExtractor
     * @throws NullPointerException if the provided {@code mapper} is {@code null}
     */
    default ToLongExcerptExtractor mapToLong(@NotNull final ToLongFunction<? super T> mapper) {
        requireNonNull(mapper);
        return (wire, index) -> {
            final T value = extract(wire, index);
            if (value == null) {
                return Long.MIN_VALUE;
            }
            return mapper.applyAsLong(value);
        };
    }

    /**
     * Creates and returns a new ExcerptExtractor consisting of the elements of this ExcerptExtractor that match
     * the provided {@code predicate}.
     *
     * @param predicate to apply to each element to determine if it
     *                  should be included
     * @return a ExcerptExtractor consisting of the elements of this ExcerptExtractor that match
     * @throws NullPointerException if the provided {@code predicate} is {@code null}
     */
    default ExcerptExtractor<T> filter(@NotNull final Predicate<? super T> predicate) {
        requireNonNull(predicate);
        return (wire, index) -> {
            final T value = extract(wire, index);
            if (value == null) {
                // The value is already filtered so just propagate the lack of a value
                return null;
            }
            return predicate.test(value)
                    ? value
                    : null;

        };
    }

    // skip

    // peek


    /**
     * Returns an ExcerptExtractor that will extract elements of the provided
     * {@code type}.
     *
     * @param type of elements to extract
     * @param <E>  element type
     * @return an ExcerptExtractor of the provided {@code type}
     * @throws NullPointerException if the provided {@code type} is {@code null}
     */
    static <E> ExcerptExtractor<E> ofType(@NotNull final Class<E> type) {
        requireNonNull(type);
        return (wire, index) -> wire
                .getValueIn()
                .object(type);
    }

    /**
     * Returns an ExcerptExtractor that will extract elements of the provided
     * {@code messageType} that was previously written using a method writer via invocations
     * of the provided {@code methodReference}.
     * <p>
     * The provided {@code methodReference} must be a true method reference (e.g. {@code Greeting:message})
     * or a corresponding lambda expression
     * (e.g. {@code (Greeting greeting, String msg) -> greeting.message(m))} ) or else the
     * result is undefined.
     *
     * @param interfaceType   interface that has at least one method that takes a single
     *                        argument parameter of type E
     * @param messageType     message type to pass to the method reference
     * @param methodReference connecting the interface type to a method that takes a single
     *                        argument parameter of type E
     * @param <I>             interface type
     * @param <E>             message type
     * @return an ExcerptExtractor that will extract elements of the provided {@code messageType}
     */
    static <I, E> ExcerptExtractor<E> ofMethod(@NotNull final Class<I> interfaceType,
                                               @NotNull final Class<E> messageType, // This type is needed for type inference
                                               @NotNull final BiConsumer<? super I, ? super E> methodReference) {
        requireNonNull(interfaceType);
        requireNonNull(messageType);
        requireNonNull(methodReference);
        return AccumulatorUtil.ofMethod(interfaceType, methodReference);
    }

}
