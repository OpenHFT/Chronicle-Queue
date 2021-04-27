package net.openhft.chronicle.queue.impl.single;

import org.jetbrains.annotations.NotNull;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * An adapter so we can use the QueueLock as an acquireAppenderCondition for backward
 * compatibility
 *
 * @deprecated This goes when QueueLock goes (.22)
 */
@Deprecated
public class QueueLockUnlockedCondition implements Condition {

    private final SingleChronicleQueue singleChronicleQueue;

    public QueueLockUnlockedCondition(SingleChronicleQueue singleChronicleQueue) {
        this.singleChronicleQueue = singleChronicleQueue;
    }

    @Override
    public void await() throws InterruptedException {
        singleChronicleQueue.queueLock().waitForLock();
    }

    @Override
    public void awaitUninterruptibly() {
        singleChronicleQueue.queueLock().waitForLock();
    }

    @Override
    public long awaitNanos(long l) {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public boolean await(long l, TimeUnit timeUnit) throws InterruptedException {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public boolean awaitUntil(@NotNull Date date) {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public void signal() {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public void signalAll() {
        throw new UnsupportedOperationException("unsupported");
    }
}
