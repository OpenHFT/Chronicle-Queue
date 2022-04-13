package net.openhft.chronicle.queue.incubator.streaming;

import net.openhft.chronicle.core.annotation.NonNegative;
import net.openhft.chronicle.queue.AppenderListener;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.internal.streaming.VanillaAppenderListenerAccumulationBuilder;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public interface Accumulation<T> extends AppenderListener {

    /**
     * Consumes an excerpt from the provided {@code wire} at the index at the provided {@code index}.
     * <p>
     * If this method throws an Exception, it is relayed to the call site.
     * Therefore, care should be taken to minimise the probability of throwing Exceptions.
     * <p>
     * If this method is referenced as an {@link AppenderListener} then the Accumulation must be
     * thread-safe.
     **/
    void onExcerpt(@NotNull Wire wire, @NonNegative long index);

    /**
     * Returns a view of an underlying accumulation.
     *
     * @return accumulation view.
     */
    @NotNull
    T accumulation();

    /**
     * Accepts the input of the provided {@code tailer } and accumulates (folds) the contents of it
     * into this Accumulation returning the last seen index or -1 if no index was seen.
     * <p>
     * This method can be used to initialise the Accumulation before appending new values.
     * <p>
     * It is the responsibility of the caller to make sure no simultaneous appenders are using
     * this Accumulation during the entire fold operation.
     *
     * @param tailer to fold (accumulate) from
     * @return the last index seen or -1 if no index was seen
     * @throws NullPointerException if the provided {@code tailer} is {@code null}
     */
    long accept(@NotNull ExcerptTailer tailer);

    interface Builder<T, A> extends net.openhft.chronicle.core.util.Builder<Accumulation<T>> {

        /**
         * Sets the Accumulator for this Builder replacing any previous Accumulator.
         *
         * @param accumulator to apply on {@link AppenderListener#onExcerpt(Wire, long)} events.
         * @return this Builder
         * @throws NullPointerException if the provided {@code accumulator } is {@code null}
         */
        @NotNull
        Builder<T, A> withAccumulator(@NotNull Builder.Accumulator<? super A> accumulator);

        /**
         * Adds a viewer to this Builder potentially provided a protected view of the underlying accumulation
         * where the view is <em>applied once</em>.
         * <p>
         * The provided {@code viewer} is only called once upon creation of the accumulation so the viewer must be a
         * true view of an underlying object and <em>not a copy</em>.
         * <p>
         * The provided viewer must not return {@code null}.
         * <p>
         * Example of valid viewers are:
         * <ul>
         *     <li>{@link Collections#unmodifiableMap(Map)}</li>
         *     <li>{@link Collections#unmodifiableList(List)} </li>
         *     <li>{@link Collections#unmodifiableSet(Set)} </li>
         * </ul>
         * <p>
         * Amy number of views can be added to the builder.
         *
         * @param viewer to add (non-null)
         * @param <R>    new view type
         * @return this Builder
         * @throws NullPointerException if the provided viewer is {@code null}.
         */
        @NotNull <R> Builder<R, A> addViewer(@NotNull Function<? super T, ? extends R> viewer);

        /**
         * Adds a mapper to this Builder potentially provided a protected view of the underlying accumulation
         * where the view is <em>applied on every {@link Accumulation#accumulation()} access</em>.
         * <p>
         * The provided {@code mapper} is called on each access of the aggregation effectively allowing a
         * restricted view the underlying accumulation.
         * <p>
         * The provided {@code mapper} must not return {@code null}.
         * <p>
         * Example of valid viewers are:
         * <ul>
         *     <li>{@link AtomicLong#get()}</li>
         *     <li>{@link AtomicInteger#get()}</li>
         * </ul>
         * <p>
         * Amy number of mappers can be added to the builder.
         *
         * @param mapper to add
         * @param <R>    new Viewer type
         * @return this Builder
         * @throws NullPointerException if the provided mapper is {@code null}.
         */
        @NotNull <R> Builder<MapperTo<R>, A> withMapper(@NotNull Function<? super T, ? extends R> mapper);

        /**
         * {@inheritDoc}
         *
         * @return a new Accumulation
         * @throws IllegalStateException if no Accumulator has been defined.
         */
        @Override
        @NotNull Accumulation<T> build();

        @FunctionalInterface
        interface Accumulator<A> {

            /**
             * Accumulates (folds) the provided {@code wire} and {@code index} into
             * the provided {@code accumulation}.
             *
             * @param accumulation to fold values into
             * @param wire         to accumulate (fold)
             * @param index        to accumulate (fold)
             */
            void accumulate(@NotNull A accumulation, @NotNull Wire wire, @NonNegative long index);

            /**
             * Creates and returns a new Accumulator that will first extract messages using the
             * provided {@code extractor} before the provided {@code downstream} accumulator is applied.
             * <p>
             * If the provided {@code extractor} returns null, the element will be ignored.
             *
             * @param extractor  extractor used to extract messages.
             * @param downstream operation to apply on the Accumulator for each element of type E.
             * @param <E>        element type
             * @return this Builder
             * @throws NullPointerException if any of the provided parameters are {@code null}
             */
            @NotNull
            static <A, E>
            Builder.Accumulator<A> reducing(@NotNull ExcerptExtractor<? extends E> extractor,
                                            @NotNull BiConsumer<? super A, ? super E> downstream) {
                requireNonNull(extractor);
                requireNonNull(downstream);
                return (accumulation, wire, index) -> {
                    final E value = extractor.extract(wire, index);
                    if (value != null) {
                        downstream.accept(accumulation, value);
                    }
                };
            }

            /**
             * Creates and returns a new Accumulator that merges values into a Map whose keys and values
             * are the result of applying the provided extractors to the input messages.
             * <p>
             * If the provided {@code extractor} returns null, the element will be ignored.
             * <p>
             * If the mapped keys contains duplicates (according to Object.equals(Object)), the
             * value mapping function is applied to each equal element, and the results are merged using
             * the provided {@code mergeFunction}.
             *
             * @param extractor      used to extract elements from queue excerpts
             * @param keyExtractor   a mapping function to produce keys from elements.
             * @param valueExtractor a mapping function to produce values from elements.
             * @param mergeFunction  a merge function, used to resolve collisions between values associated with the same key,
             *                       as supplied to Map.merge(Object, Object, BiFunction)
             * @param <A>            Underlying accumulator type
             * @param <E>            element type
             * @param <K>            key type
             * @param <V>            value type
             * @return a new Accumulator
             * @throws NullPointerException if any of the provided parmeters are {@code null}
             */
            @NotNull
            static <A extends Map<K, V>, E, K, V>
            Builder.Accumulator<A> merging(@NotNull final ExcerptExtractor<? extends E> extractor,
                                           @NotNull final Function<? super E, ? extends K> keyExtractor,
                                           @NotNull final Function<? super E, ? extends V> valueExtractor,
                                           @NotNull final BinaryOperator<V> mergeFunction) {
                requireNonNull(extractor);
                requireNonNull(keyExtractor);
                requireNonNull(valueExtractor);
                requireNonNull(mergeFunction);
                return (accumulation, wire, index) -> {
                    final E value = extractor.extract(wire, index);
                    if (value != null) {
                        accumulation.merge(keyExtractor.apply(value),
                                valueExtractor.apply(value),
                                mergeFunction);
                    }
                };
            }

            /**
             * Returns a merger that will replace an existing value with the latest value.
             *
             * @param <V> value type
             * @return a merger that will replace values
             */
            static <V> BinaryOperator<V> replacingMerger() {
                return (u, v) -> v;
            }

            /**
             * Returns a merger that will retain an existing value and discard the latest value.
             *
             * @param <V> value type
             * @return a merger that will retain values
             */
            static <V> BinaryOperator<V> retainingMerger() {
                return (u, v) -> u;
            }

            /**
             * Returns a merger that will throw an Exception if duplicate keys are detected.
             *
             * @param <V> value type
             * @return a merger that will throw an Exception if duplicate keys are detected
             */
            static <V> BinaryOperator<V> throwingMerger() {
                return (u, v) -> {
                    throw new IllegalStateException(String.format("Duplicate key for value %s", u));
                };
            }

            /**
             * Creates and returns a new long viewer that is guaranteed to operate with no auto-boxing.
             *
             * @param extractor to apply to get long values
             * @param <A>       Accumulation type
             * @return a new long viewer
             */
            static <A> Function<A, LongSupplier> longViewer(@NotNull final ToLongFunction<A> extractor) {
                requireNonNull(extractor);
                return a -> new Accumulations.LongViewer<>(a, extractor);
            }

        }

    }

    @FunctionalInterface
    interface MapperTo<T> {
        /**
         * Returns the view of this Viewer.
         *
         * @return the view
         */
        T map();

        @NotNull
        default <R> MapperTo<R> flatMap(@NotNull final Function<? super T, ? extends R> mapper) {
            requireNonNull(mapper);
            return () -> mapper.apply(map());
        }

    }

    /**
     * Creates and returns a new Builder for Accumulation objects.
     *
     * @param supplier used to create the underlying accumulation (e.g. {@code AtomicReference::new}).
     * @param <T>      type of the underlying accumulation.
     * @return a new builder.
     * @throws NullPointerException if the provided {@code supplier} is {@code null}
     */
    @NotNull
    static <T> Builder<T, T> builder(@NotNull final Supplier<? extends T> supplier) {
        requireNonNull(supplier);
        return new VanillaAppenderListenerAccumulationBuilder<>(supplier);
    }

    /**
     * Creates and returns a new Builder for Accumulation objects of type Collection.
     *
     * @param supplier used to create the underlying accumulation (e.g.
     *                 {@code Collections.synchronizedList(new ArrayList<>()}).
     * @param <E>      element type
     * @param <T>      type of the underlying accumulation.
     * @return a new builder.
     * @throws NullPointerException if any of the provided parameters are {@code null}
     */
    @NotNull
    static <T extends Collection<E>, E>
    Builder<T, T> builder(@NotNull final Supplier<? extends T> supplier,
                          @NotNull final Class<? super E> elementType) {
        requireNonNull(supplier);
        requireNonNull(elementType);
        return builder(supplier);
    }

    /**
     * Creates and returns a new Builder for Accumulation objects of type Map.
     *
     * @param supplier used to create the underlying accumulation (e.g. {@code ConcurrentMap::new}).
     * @param <T>      type of the underlying accumulation.
     * @param <K>      key type
     * @param <V>      value type
     * @return a new builder.
     * @throws NullPointerException if any of the provided parameters are {@code null}
     */
    @NotNull
    static <T extends Map<K, V>, K, V>
    Builder<T, T> builder(@NotNull final Supplier<? extends T> supplier,
                          @NotNull final Class<? super K> keyType,
                          @NotNull final Class<? super V> valueType) {
        requireNonNull(supplier);
        requireNonNull(keyType);
        requireNonNull(valueType);
        return builder(supplier);
    }
}
