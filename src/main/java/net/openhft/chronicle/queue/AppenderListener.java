package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.annotation.NonNegative;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.internal.appenderlistener.VanillaAppenderListenerAccumulationBuilder;
import net.openhft.chronicle.wire.TriConsumer;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

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

    interface Accumulation<T> extends AppenderListener {

        /**
         * Returns a view of an underlying accumulation.
         *
         * @return accumulation view.
         */
        @NotNull
        T accumulation();

        /**
         * Folds the contents of the provided {@code tailer} and accumulates every consumable
         * value into this Accumulation returning the last seen index or -1 if no index was seen.
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
        long fold(@NotNull ExcerptTailer tailer);

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
             * Sets the Accumulator for this Builder replacing any previous Accumulator.
             * <p>
             * The Accumulator consists of two steps:
             * 1) The provided {@code extractor is applied} yielding an element of type E.
             * 2) The element is accepted by the provided {@code accumulator}.
             *
             * @param accumulator to apply on {@link AppenderListener#onExcerpt(Wire, long)} events.
             * @param <E>         element type
             * @return this Builder
             * @throws NullPointerException if any of the provided parameters are {@code null}
             */
            @NotNull <E> Builder<T, A> withAccumulator(@NotNull Extractor<? extends E> extractor,
                                                       @NotNull BiConsumer<? super A, ? super E> accumulator);

            /**
             * Sets the Accumulator for this Builder replacing any previous Accumulator.
             * <p>
             * The Accumulator consists of three steps:
             * 1) The provided {@code keyExtractor is applied} yielding a key of type K.
             * 2) The provided {@code valueExtractor is applied} yielding a value of type V.
             * 3) The kay and value are accepted by the provided {@code accumulator}.
             *
             * @param accumulator to apply on {@link AppenderListener#onExcerpt(Wire, long)} events.
             * @param <K>         key type
             * @param <V>         value type
             * @return this Builder
             * @throws NullPointerException if any of the provided parameters are {@code null}
             */
            @NotNull <K, V> Builder<T, A> withAccumulator(@NotNull Extractor<? extends K> keyExtractor,
                                                          @NotNull Extractor<? extends V> valueExtractor,
                                                          @NotNull TriConsumer<? super A, ? super K, ? super V> accumulator);

            /**
             * Adds a viewer to this Builder potentially provided a protected view of the underlying accumulation
             * where the view is <em>applied once</em>.
             * <p>
             * The provided viewer is only called once upon creation of the accumulation so the viewer must be a
             * true view of an underlying object and <em>not a copy</em>.
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
             * @param viewer to add
             * @param <R>    new view type
             * @return this Builder
             * @throws NullPointerException if the provided viewer is {@code null}.
             */
            @NotNull <R> Builder<R, A> addViewer(@NotNull Function<? super T, ? extends R> viewer);

            /**
             * Adds a mapper to this Builder potentially provided a protected view of the underlying accumulation
             * where the view is <em>applied on every {@link Accumulation#accumulation()} access</em>.
             * <p>
             * The provided mapper is called on each access of the aggregation effectively allowing a restricted view
             * of an underlying object.
             * <p>
             * Example of valid viewers are:
             * <ul>
             *     <li>{@link AtomicLong#get()}</li>
             *     <li>{@link Map#get(Object)} </li>
             * </ul>
             * <p>
             * Amy number of mappers can be added to the builder.
             *
             * @param mapper to add
             * @param <R>    new Viewer type
             * @return this Builder
             * @throws NullPointerException if the provided mapper is {@code null}.
             */
            @NotNull <R> Builder<Viewer<R>, A> addMapper(@NotNull Function<? super T, ? extends R> mapper);

            /**
             * {@inheritDoc}
             *
             * @return a new Accumulation
             * @throws IllegalStateException if no Accumulator has been defined.
             */
            @Override
            @NotNull Accumulation<T> build();

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
            }

            interface Extractor<T> {

                /**
                 * Extracts a valye of type T from the provided {@code wire} and {@code index}.
                 *
                 * @param wire  to accumulate (fold)
                 * @param index to accumulate (fold)
                 * @return extracted value
                 */
                T extract(@NotNull Wire wire, @NonNegative long index);
            }

        }

        interface Viewer<T> {
            T view();
        }

        /**
         * Creates and returns a new Builder for Accumulation objects.
         *
         * @param supplier used to create the underlying accumulation (e.g. {@code AtomicReference::new}).
         * @param <T>      type of the underlying accumulation.
         * @return a new builder.
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
         */
        @NotNull
        static <T extends Collection<E>, E> Builder<T, T> builder(@NotNull final Supplier<? extends T> supplier,
                                                                  @NotNull final Class<? super E> elementType) {
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
         */
        @NotNull
        static <T extends Map<K, V>, K, V> Builder<T, T> builder(@NotNull final Supplier<? extends T> supplier,
                                                                 @NotNull final Class<? super K> keyType,
                                                                 @NotNull final Class<? super V> valueType) {
            requireNonNull(keyType);
            requireNonNull(valueType);
            return builder(supplier);
        }
    }

}