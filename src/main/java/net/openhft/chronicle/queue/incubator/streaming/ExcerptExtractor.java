package net.openhft.chronicle.queue.incubator.streaming;

import net.openhft.chronicle.core.annotation.NonNegative;
import net.openhft.chronicle.queue.internal.streaming.ExcerptExtractorBuilder;
import net.openhft.chronicle.queue.internal.streaming.IncubatorWarning;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.*;

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
     * Generic builder that can be used to build ExcerptExtractor objects of common types.
     *
     * @param <E> element type to extract
     */
    interface Builder<E> extends net.openhft.chronicle.core.util.Builder<ExcerptExtractor<E>> {

        /**
         * Specifies a {@code supplier} of element that shall be reused when extracting elements from excerpts.
         * <p>
         * By default, thread local reuse objects are created on demand but this can be changed by means of the
         * {@link #withThreadConfinedReuse()} method.
         * <p>
         * The provided supplier must provide distinct objects on each invocation. This can be accomplished
         * , for example, by referencing {@code Foo:new}.
         *
         * @param supplier to call when reuse object are needed (non-null)
         * @return this Builder
         */
        @NotNull
        Builder<E> withReusing(@NotNull Supplier<? extends E> supplier);

        /**
         * Specifies that only one reuse object, confined to the first using thread, shall be reused.
         * <p>
         * The ExcerptExtractor is guaranteed to prevent accidental concurrent thread access by throwing
         * an {@link IllegalStateException} if accessed by a foreign thread.
         *
         * @return this Builder
         */
        @NotNull
        Builder<E> withThreadConfinedReuse();

        /**
         * Specifies an {@code interfaceType } that was previously used to write messages of type E using
         * a method writer via invocations of the provided {@code methodReference}.
         * <p>
         * The provided {@code methodReference} must be a true method reference (e.g. {@code Greeting:message})
         * or a corresponding lambda expression
         * (e.g. {@code (Greeting greeting, String msg) -> greeting.message(m))} ) or else the
         * result is undefined.
         *
         * @param interfaceType   interface that has at least one method that takes a single
         *                        argument parameter of type E
         * @param methodReference connecting the interface type to a method that takes a single
         *                        argument parameter of type E
         * @param <I>             interface type
         * @return this Builder
         */
        @NotNull <I> Builder<E> withMethod(@NotNull final Class<I> interfaceType,
                                           @NotNull final BiConsumer<? super I, ? super E> methodReference);

    }

    /**
     * Creates and returns a new Builder that can be used to create ExcerptExtractor objects
     * of the provided {@code elementType}.
     *
     * @param elementType type of element to extract
     * @param <E>         element type
     * @return a new Builder
     */
    static <E> Builder<E> builder(@NotNull final Class<E> elementType) {
        requireNonNull(elementType);
        return new ExcerptExtractorBuilder<>(elementType);
    }

}