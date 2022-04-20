package net.openhft.chronicle.queue.internal.streaming;

import net.openhft.chronicle.core.util.ThreadConfinementAsserter;
import net.openhft.chronicle.queue.incubator.streaming.ExcerptExtractor;
import net.openhft.chronicle.queue.incubator.streaming.ExcerptExtractor.Builder;
import org.jetbrains.annotations.NotNull;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public final class ExcerptExtractorBuilder<E> implements ExcerptExtractor.Builder<E> {

    static {
        IncubatorWarning.warnOnce();
    }

    private final Class<E> elementType;

    private Supplier<? extends E> supplier;
    private boolean threadConfinedReuse;
    private MethodRef<Object, E> methodRef;

    public ExcerptExtractorBuilder(@NotNull final Class<E> elementType) {
        this.elementType = requireNonNull(elementType);
    }

    @NotNull
    @Override
    public Builder<E> withReusing(@NotNull Supplier<? extends E> supplier) {
        this.supplier = requireNonNull(supplier);
        return this;
    }

    @NotNull
    @Override
    public Builder<E> withThreadConfinedReuse() {
        threadConfinedReuse = true;
        return this;
    }

    @SuppressWarnings("unchecked")
    @NotNull
    @Override
    public <I> Builder<E> withMethod(@NotNull final Class<I> interfaceType,
                                     @NotNull final BiConsumer<? super I, ? super E> methodReference) {
        methodRef = (MethodRef<Object, E>) new MethodRef<>(interfaceType, methodReference);
        return this;
    }

    @NotNull
    @Override
    public ExcerptExtractor<E> build() {

        if (methodRef != null) {
            if (supplier == null) {
                // () -> null means null will be used as reuse meaning new objects are created
                return ExcerptExtractorUtil.ofMethod(methodRef.interfaceType(), methodRef.methodReference(), () -> null);
            }
            return ExcerptExtractorUtil.ofMethod(methodRef.interfaceType(), methodRef.methodReference(), guardedSupplier());
        }

        if (supplier == null) {
            return (wire, index) -> wire
                    .getValueIn()
                    .object(elementType); // No lambda capture
        } else {
            final Supplier<? extends E> internalSupplier = guardedSupplier();
            return (wire, index) -> {
                final E using = internalSupplier.get();
                return wire
                        .getValueIn()
                        .object(using, elementType); // Lambda capture
            };
        }
    }

    Supplier<E> guardedSupplier() {
        // Either of these protected Suppliers are used
        return threadConfinedReuse
                ? new ThreadConfinedSupplier<>(supplier)
                : new ThreadLocalSupplier<>(supplier);
    }

    static final class ThreadLocalSupplier<E> implements Supplier<E> {

        private final ThreadLocal<E> threadLocal;

        public ThreadLocalSupplier(@NotNull final Supplier<? extends E> supplier) {
            this.threadLocal = ThreadLocal.withInitial(supplier);
        }

        @Override
        public E get() {
            return threadLocal.get();
        }
    }

    static final class ThreadConfinedSupplier<E> implements Supplier<E> {

        private final ThreadConfinementAsserter asserter = ThreadConfinementAsserter.createEnabled();
        private final E delegate;

        public ThreadConfinedSupplier(@NotNull final Supplier<? extends E> supplier) {
            // Eagerly create the reuse object
            this.delegate = requireNonNull(supplier.get());
        }

        @Override
        public E get() {
            asserter.assertThreadConfined();
            return delegate;
        }
    }

    private static final class MethodRef<I, E> {

        final Class<I> interfaceType;
        final BiConsumer<I, E> methodReference;

        @SuppressWarnings("unchecked")
        public MethodRef(@NotNull final Class<I> interfaceType,
                         @NotNull final BiConsumer<? super I, ? super E> methodReference) {
            this.interfaceType = interfaceType;
            this.methodReference = (BiConsumer<I, E>) methodReference;
        }

        public Class<I> interfaceType() {
            return interfaceType;
        }

        public BiConsumer<I, E> methodReference() {
            return methodReference;
        }
    }

}