package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.HandlerPriority;

public final class PretouchHandler implements EventHandler {
    private ChronicleQueue queue;

    public PretouchHandler(ChronicleQueue queue) {
        this.queue = queue;
    }

    @Override
    public boolean action() {
        queue.acquireAppender().pretouch();
        return false;
    }

    @Override
    public HandlerPriority priority() {
        return HandlerPriority.MONITOR;
    }
}
