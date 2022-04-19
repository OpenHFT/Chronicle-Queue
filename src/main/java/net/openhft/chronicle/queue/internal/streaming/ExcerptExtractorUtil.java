package net.openhft.chronicle.queue.internal.streaming;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.queue.incubator.streaming.ExcerptExtractor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public final class ExcerptExtractorUtil {

    // Suppresses default constructor, ensuring non-instantiability.
    private ExcerptExtractorUtil() {
    }

    public static <I, E>
    ExcerptExtractor<E> ofMethod(@NotNull final Class<I> type,
                                 @NotNull final BiConsumer<? super I, ? super E> methodReference,
                                 @Nullable final Supplier<? extends E> supplier) {
        final MethodNameAndMessageType<E> info = methodOf(type, methodReference);
        final String expectedEventName = info.name();
        final Class<E> elementType = info.messageType();
        final StringBuilder eventName = new StringBuilder();
        return (wire, index) -> {
            wire.startEvent();
            try {
                final Bytes<?> bytes = wire.bytes();
                while (bytes.readRemaining() > 0) {
                    if (wire.isEndEvent()) {
                        break;
                    }
                    final long start = bytes.readPosition();

                    wire.readEventName(eventName);
                    if (StringUtils.isEqual(expectedEventName, eventName)) {
                        final E using = supplier.get();
                        return wire
                                .getValueIn()
                                .object(using, elementType);
                    }
                    wire.consumePadding();
                    if (bytes.readPosition() == start) {
                        break;
                    }
                }
            } finally {
                wire.endEvent();
            }
            // Nothing to return. There are no messages of type E
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
        private final String name;
        private final Class<M> messageType;

        public MethodNameAndMessageType(@NotNull final String name,
                                        @NotNull final Class<M> messageType) {
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