package net.openhft.chronicle.queue.internal.appenderlistener;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.queue.AppenderListener.Accumulation.Builder.ExcerptExtractor;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public final class AccumulatorUtil {

    // Suppresses default constructor, ensuring non-instantiability.
    private AccumulatorUtil() {
    }
/*
    @NotNull
    public static <I, A, E>
    Accumulator<A> reducingMethod(@NotNull final Class<I> type,
                                  @NotNull final BiConsumer<? super I, ? super E> methodReference,
                                  @NotNull final BiConsumer<? super A, ? super E> downstream) {
        requireNonNull(type);
        requireNonNull(methodReference);
        requireNonNull(downstream);

        final MethodNameAndMessageType<E> info = methodOf(type, methodReference);
        final Accumulator<A> delegate = reducingType(info.messageType(), downstream);
        return reducingMethodHelper(type, methodReference, info, delegate);
    }


    @NotNull
    public static <I, A extends Map<K, V>, E, K, V>
    Accumulator<A> mappingMethod(@NotNull final Class<I> type,
                                 @NotNull final BiConsumer<? super I, ? super E> methodReference,
                                 @NotNull final Function<? super E, ? extends K> keyExtractor,
                                 @NotNull final Function<? super E, ? extends V> valueExtractor,
                                 @NotNull final BinaryOperator<V> mergeFunction) {
        requireNonNull(type);
        requireNonNull(methodReference);
        requireNonNull(keyExtractor);
        requireNonNull(valueExtractor);
        requireNonNull(mergeFunction);

        final MethodNameAndMessageType<E> info = methodOf(type, methodReference);
        final Accumulator<A> delegate = mappingType(info.messageType(), keyExtractor, valueExtractor, mergeFunction);
        return reducingMethodHelper(type, methodReference, info, delegate);
    }*/
/*

    @NotNull
    static <I, A, E>
    Accumulator<A> reducingMethodHelper(@NotNull final Class<I> type,
                                        @NotNull final BiConsumer<? super I, ? super E> methodReference,
                                        @NotNull final AccumulatorUtil.MethodNameAndMessageType<E> info,
                                        @NotNull final Accumulator<A> delegate) {
        requireNonNull(type);
        requireNonNull(methodReference);
        requireNonNull(info);
        requireNonNull(delegate);

        final String expectedEventName = info.name();
        final StringBuilder eventName = new StringBuilder();
        return ((accumulation, wire, index) -> {
            wire.startEvent();
            try {
                Bytes<?> bytes = wire.bytes();
                while (bytes.readRemaining() > 0) {
                    if (wire.isEndEvent())
                        break;
                    long start = bytes.readPosition();

                    wire.readEventName(eventName);
                    if (StringUtils.isEqual(expectedEventName, eventName)) {
                        delegate.accumulate(accumulation, wire, index);
                                    */
/*final M m = wire.readEvent(messageType);
                                    downstream.accept(accumulation, m);*//*

                        return;
                    }
                    wire.consumePadding();
                    if (bytes.readPosition() == start) {
                        //Jvm.warn().on(AppenderListener.class, "Failed to progress reading " + bytes.readRemaining() + " bytes left.");
                        break;
                    }
                }
            } finally {
                wire.endEvent();
            }
        });
    }
*/


    public static <I, E> ExcerptExtractor<E> ofMethod(@NotNull final Class<I> type,
                                                      @NotNull final BiConsumer<? super I, ? super E> methodReference) {
        final MethodNameAndMessageType<E> info = methodOf(type, methodReference);
        final String expectedEventName = info.name();
        final Class<E> elementType = info.messageType();
        final StringBuilder eventName = new StringBuilder();
        return (wire, index) -> {
            wire.startEvent();
            try {
                Bytes<?> bytes = wire.bytes();
                while (bytes.readRemaining() > 0) {
                    if (wire.isEndEvent())
                        break;
                    long start = bytes.readPosition();

                    wire.readEventName(eventName);
                    if (StringUtils.isEqual(expectedEventName, eventName)) {
                        final E value = wire.getValueIn().object(elementType);
                        return value;
                    }
                    wire.consumePadding();
                    if (bytes.readPosition() == start) {
                        //Jvm.warn().on(AppenderListener.class, "Failed to progress reading " + bytes.readRemaining() + " bytes left.");
                        break;
                    }
                }
            } finally {
                wire.endEvent();
            }
            // Nothing to return :-(
            return null;
        };

    }


    public static <I, M>
    MethodNameAndMessageType<M> methodOf(@NotNull final Class<I> type,
                                         @NotNull final BiConsumer<? super I, ? super M> methodReference) {

        final AtomicReference<MethodNameAndMessageType<M>> method = new AtomicReference<>();
        @SuppressWarnings("unchecked") final I proxy = (I) Proxy.newProxyInstance(type.getClassLoader(), new Class[]{type}, (p, m, args) -> {
            if (args == null || args.length != 1) {
                throw new IllegalArgumentException("The provided method reference does not take exactly one parameter");
            }
            final String methodName = m.getName();
            @SuppressWarnings("unchecked") final Class<M> messageType = (Class<M>) m.getParameters()[0].getType();
            method.set(new MethodNameAndMessageType<>(methodName, messageType));
            return p;
        });

        // Invoke the provided methodReference to see which method was actually called.
        methodReference.accept(proxy, null);

        return requireNonNull(method.get());
    }

    public static final class MethodNameAndMessageType<M> {
        final String name;
        final Class<M> messageType;

        public MethodNameAndMessageType(String name, Class<M> messageType) {
            this.name = name;
            this.messageType = messageType;
        }

        public String name() {
            return name;
        }

        public Class<M> messageType() {
            return messageType;
        }
    }

}