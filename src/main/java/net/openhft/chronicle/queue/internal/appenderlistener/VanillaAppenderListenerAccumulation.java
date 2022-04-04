package net.openhft.chronicle.queue.internal.appenderlistener;

import net.openhft.chronicle.queue.AppenderListener;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

final class VanillaAppenderListenerAccumulation<A, T> implements AppenderListener.Accumulation<T> {

    private final A accumulation;
    private final T view;
    private final Builder.Accumulator<? super A> accumulator;

    VanillaAppenderListenerAccumulation(@NotNull final Supplier<? extends A> supplier,
                                        @NotNull final Function<? super A, ? extends T> viewer,
                                        @NotNull final AppenderListener.Accumulation.Builder.Accumulator<? super A> accumulator) {
        requireNonNull(supplier);
        requireNonNull(viewer);
        requireNonNull(accumulator);
        this.accumulation = requireNonNull(supplier.get());
        this.view = viewer.apply(accumulation);
        this.accumulator = accumulator;
    }

    @Override
    public void onExcerpt(@NotNull final Wire wire,
                          final long index) {
        accumulator.accumulate(accumulation, wire, index);
    }

    @Override
    public @NotNull T accumulation() {
        return view;
    }

}