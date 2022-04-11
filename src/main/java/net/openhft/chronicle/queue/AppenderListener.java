package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.annotation.NonNegative;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.internal.appenderlistener.AccumulatorUtil;
import net.openhft.chronicle.queue.internal.appenderlistener.VanillaAppenderListenerAccumulationBuilder;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

/**
 * A listener interface for receiving events when excerpt
 * are durably persisted to a queue.
 * <p>
 * Implementations of this interface must be thread-safe as further discussed
 * under {@link #onExcerpt(Wire, long)}.
 *
 * @see SingleChronicleQueueBuilder#appenderListener(AppenderListener)
 */
@FunctionalInterface
public interface AppenderListener {

    /**
     * Invoked after an excerpt has been durably persisted to a queue.
     * <p>
     * The Thread that invokes this method is unspecified and may change, even
     * from invocation to invocation. This means implementations must ensure thread-safety
     * to guarantee correct behaviour. In particular, <em>it is an error to assume
     * the appending Thread will always be used do invoke this method</em>.
     * <p>
     * If this method throws an Exception, it is relayed to the call site.
     * Therefore, care should be taken to minimise the probability of throwing Exceptions.
     * <p>
     * It is imperative that actions performed by the method are as performant
     * as possible as any delay incurred by the invocation of this method
     * will carry over to the appender used to actually persist the message
     * (i.e. both for synchronous and asynchronous appenders actually storing messages).
     * <p>
     * No promise is given as to when this method is invoked. However, eventually
     * the method will be called for each excerpt persisted to the queue.
     * <p>
     * No promise is given as to the order in which invocations are made of this method.
     *
     * @param wire  representing access to the excerpt that was stored (non-null).
     * @param index in the queue where the except was placed (non-negative)
     */
    void onExcerpt(@NotNull Wire wire, @NonNegative long index);

    /**
     * Returns a composed AppenderListener that first accepts excerpts to this AppenderListener,
     * and then accepts excerpts to the {@code after} AppenderListener.
     * If execution of either listener throws an exception, it is relayed to
     * the caller of the composed AppenderListener.
     * <p>
     * Care should be taken to only create composed listeners that are performant.
     *
     * @param after the AppenderListener to accept excerpts after this AppenderListener
     * @return a composed AppenderListener
     * @throws NullPointerException if {@code after} is {@code null }
     */
    @NotNull
    default AppenderListener andThen(@NotNull final AppenderListener after) {
        requireNonNull(after);
        return ((wire, index) -> {
            final long readPosition = wire.bytes().readPosition();
            this.onExcerpt(wire, index);
            // Rewind the wire to allow replaying of messages
            wire.bytes().readPosition(readPosition);
            after.onExcerpt(wire, index);
        });
    }

    interface Accumulation<T> extends AppenderListener {

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
            Builder<T, A> withAccumulator(@NotNull Accumulator<? super A> accumulator);

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
                Accumulator<A> reducing(@NotNull ExcerptExtractor<? extends E> extractor,
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
                 * Creates and returns a new Accumulator that accumulates elements into a Map whose keys and values
                 * are the result of applying the provided extractors to the input messages.
                 * <p>
                 * If the provided {@code extractor} returns null, the element will be ignored.
                 * <p>
                 * If the mapped keys contains duplicates (according to Object.equals(Object)), the
                 * value mapping function is applied to each equal element, and the results are merged using
                 * the provided {@code mergeFunction}.
                 *
                 * @param keyExtractor   a mapping function to produce keys.
                 * @param valueExtractor a mapping function to produce values.
                 * @param mergeFunction  a merge function, used to resolve collisions between values associated with the same key,
                 *                       as supplied to Map.merge(Object, Object, BiFunction)
                 * @param <A>            Underlying accumulator type
                 * @param <K>            key type
                 * @param <V>            value type
                 * @return a new Accumulator
                 * @throws NullPointerException if any of the provided parmeters are {@code null}
                 */
                @NotNull
                static <A extends Map<K, V>, E, K, V>
                Accumulator<A> mapping(@NotNull final ExcerptExtractor<? extends E> extractor,
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
                static <A> Function<? super A, LongSupplier> longViewer(@NotNull final ToLongFunction<A> extractor) {
                    requireNonNull(extractor);
                    return a -> new Accumulations.LongViewer<>(a, extractor);
                }

            }

            @FunctionalInterface
            interface ExcerptExtractor<T> {

                /**
                 * Extracts a value of type T from the provided {@code wire} and {@code index} or else {@code null}
                 * if no value can be extracted.
                 * <p>
                 * Null may be returned if the queue was written with a method writer and there are messages in the
                 * queue but of another type.
                 *
                 * @param wire  to accumulate (fold)
                 * @param index to accumulate (fold)
                 * @return extracted value
                 */
                @Nullable
                T extract(@NotNull Wire wire, @NonNegative long index);

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
                    return (wire, index) -> mapper.apply(
                            extract(wire, index)
                    );
                }

                /**
                 * Returns a ExcerptExtractor consisting of the elements of this ExcerptExtractor that match
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
        static <T extends Collection<E>, E> Builder<T, T> builder(@NotNull final Supplier<? extends T> supplier,
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
        static <T extends Map<K, V>, K, V> Builder<T, T> builder(@NotNull final Supplier<? extends T> supplier,
                                                                 @NotNull final Class<? super K> keyType,
                                                                 @NotNull final Class<? super V> valueType) {
            requireNonNull(supplier);
            requireNonNull(keyType);
            requireNonNull(valueType);
            return builder(supplier);
        }
    }
}