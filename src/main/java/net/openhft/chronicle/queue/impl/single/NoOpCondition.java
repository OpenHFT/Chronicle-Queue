package net.openhft.chronicle.queue.impl.single;

import org.jetbrains.annotations.NotNull;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * A condition that is always true
 */
public final class NoOpCondition implements Condition {

    public static final NoOpCondition INSTANCE = new NoOpCondition();

    private NoOpCondition() {}

    @Override
    public void await() throws InterruptedException {
    }

    @Override
    public void awaitUninterruptibly() {
    }

    @Override
    public long awaitNanos(long l) {
        return l;
    }

    @Override
    public boolean await(long l, TimeUnit timeUnit) throws InterruptedException {
        return true;
    }

    @Override
    public boolean awaitUntil(@NotNull Date date) {
        return true;
    }

    @Override
    public void signal() {
    }

    @Override
    public void signalAll() {
    }
}
