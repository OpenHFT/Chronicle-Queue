package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.AbstractReferenceCounted;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.core.onoes.Slf4jExceptionHandler;
import net.openhft.chronicle.core.threads.CleaningThread;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.wire.MessageHistory;
import org.junit.After;
import org.junit.Before;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Predicate;

public class QueueTestCommon {
    protected ThreadDump threadDump;
    protected Map<ExceptionKey, Integer> exceptions;
    private final Map<Predicate<ExceptionKey>, String> expectedExceptions = new LinkedHashMap<>();

    @Before
    public void clearMessageHistory() {
        MessageHistory.get().reset();
    }

    @Before
    public void enableReferenceTracing() {
        AbstractReferenceCounted.enableReferenceTracing();
    }

    public void assertReferencesReleased() {
        AbstractReferenceCounted.assertReferencesReleased();
    }

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @Before
    public void recordExceptions() {
        exceptions = Jvm.recordExceptions();
    }

    public void expectException(String message) {
        expectException(k -> k.message.contains(message) || (k.throwable != null && k.throwable.getMessage().contains(message)), message);
    }

    public void expectException(Predicate<ExceptionKey> predicate, String description) {
        expectedExceptions.put(predicate, description);
    }

    public void checkExceptions() {
        for (Map.Entry<Predicate<ExceptionKey>, String> expectedException : expectedExceptions.entrySet()) {
            if (!exceptions.keySet().removeIf(expectedException.getKey()))
                Slf4jExceptionHandler.WARN.on(getClass(), "No error for " + expectedException.getValue());
        }
        expectedExceptions.clear();
        if (hasExceptions(exceptions)) {
            Jvm.dumpException(exceptions);
            Jvm.resetExceptionHandlers();
            throw new AssertionError(exceptions.keySet());
        }
    }

    protected boolean hasExceptions(Map<ExceptionKey, Integer> exceptions) {
        return Jvm.hasException(this.exceptions);
    }

    @After
    public void resetClock() {
        SystemTimeProvider.CLOCK = SystemTimeProvider.INSTANCE;
    }

    @After
    public void afterChecks() {
        CleaningThread.performCleanup(Thread.currentThread());

        // find any discarded resources.
        System.gc();
        AbstractCloseable.waitForCloseablesToClose(100);

        assertReferencesReleased();
        checkThreadDump();
        checkExceptions();
    }
}
