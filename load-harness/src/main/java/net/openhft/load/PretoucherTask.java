package net.openhft.load;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class PretoucherTask implements Runnable {
    private final SingleChronicleQueue queue;
    private final long intervalMillis;

    public PretoucherTask(final SingleChronicleQueue queue, final long intervalMillis) {
        this.queue = queue;
        this.intervalMillis = intervalMillis;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("pretoucher-0x" +
                Integer.toHexString(System.identityHashCode(this)) + "-" + queue.file().getPath());
        while (!Thread.currentThread().isInterrupted()) {
            queue.acquireAppender().pretouch();

            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(intervalMillis));
        }
    }
}