package net.openhft.chronicle.queue.internal.appenderlistener;

import net.openhft.chronicle.queue.AppenderListener;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public final class VanillaAppenderListenerAccumulationBuilder<T, A> implements AppenderListener.Accumulation.Builder<T, A> {

    private final Supplier<? extends A> supplier;
    private Accumulator<? super A> accumulator;
    private Function<? super A, ? extends T> viewer;
    private boolean built;

    public VanillaAppenderListenerAccumulationBuilder(@NotNull final Supplier<? extends A> supplier) {
        this.supplier = requireNonNull(supplier);
        // Initially, A and T are of the same type so this is always safe.
        // The view becomes the accumulation by default.
        this.viewer = a -> (T) a;
    }

    @NotNull
    @Override
    public AppenderListener.Accumulation.Builder<T, A>
    withAccumulator(@NotNull final Accumulator<? super A> accumulator) {
        this.accumulator = requireNonNull(accumulator);
        return this;
    }

    @NotNull
    @Override
    public <R> AppenderListener.Accumulation.Builder<R, A>
    addViewer(@NotNull Function<? super T, ? extends R> viewer) {
        requireNonNull(viewer);
        @SuppressWarnings("unchecked") final VanillaAppenderListenerAccumulationBuilder<R, A> newType =
                (VanillaAppenderListenerAccumulationBuilder<R, A>) this;
        // There can be several layers of viewers applied
        newType.viewer = this.viewer.andThen(viewer);
        return newType;
    }

    @NotNull
    @Override
    public <R> AppenderListener.Accumulation.Builder<AppenderListener.Accumulation.MapperTo<R>, A>
    withMapper(@NotNull Function<? super T, ? extends R> mapper) {
        requireNonNull(mapper);
        return addViewer(t -> new VanillaMapper<>(t, mapper));
    }

    @NotNull
    @Override
    public AppenderListener.Accumulation<T> build() {
        if (built) {
            throw new IllegalStateException("This builder has already been built!");
        }
        built = true;
        if (accumulator == null) {
            throw new IllegalStateException("No " + Accumulator.class.getSimpleName() + " has been defined.");
        }
        return new VanillaAppenderListenerAccumulation<>(supplier, viewer, accumulator);
    }
}
