package net.openhft.chronicle.queue.incubator.streaming;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.queue.ExcerptListener;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.internal.streaming.InternalAutoTailers;
import net.openhft.chronicle.threads.Pauser;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public final class AutoTailers {

    // Suppresses default constructor, ensuring non-instantiability.
    private AutoTailers() {
    }

    public interface CloseableRunnable extends Runnable, AutoCloseable {
        @Override
        void close();
    }

    public interface CloseableEventHandler extends EventHandler, AutoCloseable {
        @Override
        void close();
    }

    @NotNull
    public static CloseableRunnable createRunnable(@NotNull final Supplier<ExcerptTailer> tailerSupplier,
                                                   @NotNull final ExcerptListener excerptListener,
                                                   @NotNull final Supplier<Pauser> pauserSupplier) {
        requireNonNull(tailerSupplier);
        requireNonNull(excerptListener);
        requireNonNull(pauserSupplier);

        return new InternalAutoTailers.RunnablePoller(tailerSupplier, excerptListener, pauserSupplier);
    }

    @NotNull
    public static CloseableEventHandler createEventHandler(@NotNull final Supplier<ExcerptTailer> tailerSupplier,
                                                           @NotNull final ExcerptListener excerptListener) {
        requireNonNull(tailerSupplier);
        requireNonNull(excerptListener);

        return new InternalAutoTailers.EventHandlerPoller(tailerSupplier, excerptListener);
    }

}