package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.queue.impl.single.Pretoucher;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.jetbrains.annotations.NotNull;

public final class PretouchHandler implements EventHandler {
    private final Pretoucher pretoucher;
    private long lastRun = 0;

    public PretouchHandler(final SingleChronicleQueue queue) {
        this.pretoucher = new Pretoucher(queue);
    }

    @Override
    public boolean action() throws InvalidEventHandlerException {
        long now = System.currentTimeMillis();
        // don't check too often.
        if (now > lastRun + 100) {
            pretoucher.execute();
            lastRun = now;
        }
        return false;
    }

    @NotNull
    @Override
    public HandlerPriority priority() {
        return HandlerPriority.MONITOR;
    }

    public void shutdown() {
        pretoucher.shutdown();
    }
}