package net.openhft.chronicle.queue.internal.streaming;

import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.queue.ExcerptListener;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.incubator.streaming.AutoTailers;
import net.openhft.chronicle.threads.Pauser;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public final class InternalAutoTailers {

    private InternalAutoTailers() {
    }

    public static final class RunnablePoller extends AbstractPoller implements AutoTailers.CloseableRunnable {

        private final Pauser pauser;

        public RunnablePoller(@NotNull final Supplier<ExcerptTailer> tailerSupplier,
                              @NotNull final ExcerptListener excerptListener,
                              @NotNull final Supplier<Pauser> pauserSupplier) {
            super(tailerSupplier, excerptListener);
            requireNonNull(pauserSupplier);
            this.pauser = requireNonNull(pauserSupplier.get());
        }

        @Override
        public void run() {
            try {
                while (running) {
                    if (ReductionUtil.accept(tailer, excerptListener) != -1) {
                        pauser.pause();
                    }
                }
            } finally {
                tailer.close();
            }
        }
    }

    public static final class EventHandlerPoller extends AbstractPoller implements AutoTailers.CloseableEventHandler {

        public EventHandlerPoller(@NotNull final Supplier<ExcerptTailer> tailerSupplier,
                                  @NotNull final ExcerptListener excerptListener) {
            super(tailerSupplier, excerptListener);
        }

        @Override
        public boolean action() throws InvalidEventHandlerException {
            if (!running) {
                tailer.close();
                throw InvalidEventHandlerException.reusable();
            }
            return ReductionUtil.accept(tailer, excerptListener) != -1;
        }
    }

    private abstract static class AbstractPoller implements AutoCloseable {

        protected final ExcerptListener excerptListener;
        protected final ExcerptTailer tailer;
        protected volatile boolean running = true;

        protected AbstractPoller(@NotNull final Supplier<ExcerptTailer> tailerSupplier,
                                 @NotNull final ExcerptListener excerptListener) {
            requireNonNull(tailerSupplier);
            this.excerptListener = requireNonNull(excerptListener);
            this.tailer = requireNonNull(tailerSupplier.get());
        }

        @Override
        public final void close() {
            running = false;
        }
    }

}