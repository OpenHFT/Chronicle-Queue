package net.openhft.chronicle.queue.impl.single.preroucher;

import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.threads.TimedEventHandler;

import java.util.function.LongConsumer;
import java.util.function.Supplier;

public final class PeriodicUpdateEventHandler extends TimedEventHandler {
    private final Supplier<LongConsumer> methodSupplier;
    private final long periodInMicroseconds;
    private final long startTimeMillis;
    private LongConsumer periodicUpdateHandler;

    PeriodicUpdateEventHandler(final Supplier<LongConsumer> methodSupplier,
                               long startTimeMillis, final long periodInMicroseconds) {
        this.methodSupplier = methodSupplier;
        this.startTimeMillis = startTimeMillis;
        this.periodInMicroseconds = periodInMicroseconds;
    }

    @Override
    protected long timedAction() throws InvalidEventHandlerException {
        long now = System.currentTimeMillis();
        if (now < startTimeMillis)
            return now - startTimeMillis;

        periodicUpdateHandler.accept(now);

        if (periodInMicroseconds == 0)
            throw new InvalidEventHandlerException("just runs once");

        // Note that this does not account for coordinated ommission - we should return actual time until we should run next
        return periodInMicroseconds;
    }

    @Override
    public void eventLoop(final EventLoop eventLoop) {
        // called on the event-loop thread, so at this point it is safe to acquire the appender
        periodicUpdateHandler = methodSupplier.get();
    }
}
