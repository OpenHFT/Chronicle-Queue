package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
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
import java.util.stream.Collectors;

public class QueueTestCommon {
    private final Map<Predicate<ExceptionKey>, String> ignoredExceptions = new LinkedHashMap<>();
    private final Map<Predicate<ExceptionKey>, String> expectedExceptions = new LinkedHashMap<>();
    protected ThreadDump threadDump;
    protected Map<ExceptionKey, Integer> exceptions;
    protected boolean finishedNormally;

    @Before
    public void assumeFinishedNormally() {
        finishedNormally = true;
    }

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

    public void ignoreException(String message) {
        ignoreException(k -> contains(k.message, message) || (k.throwable != null && contains(k.throwable.getMessage(), message)), message);
    }

    static boolean contains(String text, String message) {
        return text != null && text.contains(message);
    }

    public void expectException(String message) {
        expectException(k -> contains(k.message, message) || (k.throwable != null && contains(k.throwable.getMessage(), message)), message);
    }

    public void ignoreException(Predicate<ExceptionKey> predicate, String description) {
        ignoredExceptions.put(predicate, description);
    }

    public void expectException(Predicate<ExceptionKey> predicate, String description) {
        expectedExceptions.put(predicate, description);
    }

    public void checkExceptions() {
        if (OS.isWindows())
            ignoreException("Read-only mode is not supported on Windows® platforms, defaulting to read/write");
        for (Map.Entry<Predicate<ExceptionKey>, String> expectedException : expectedExceptions.entrySet()) {
            if (!exceptions.keySet().removeIf(expectedException.getKey()))
                throw new AssertionError("No error for " + expectedException.getValue());
        }
        expectedExceptions.clear();
        for (Map.Entry<Predicate<ExceptionKey>, String> ignoredException : ignoredExceptions.entrySet()) {
            if (!exceptions.keySet().removeIf(ignoredException.getKey()))
                Slf4jExceptionHandler.DEBUG.on(getClass(), "Ignored " + ignoredException.getValue());
        }
        ignoredExceptions.clear();
        for (String msg : "Shrinking ,Allocation of , ms to add mapping for ,jar to the classpath, ms to pollDiskSpace for , us to linearScan by position from ,File released ,Overriding roll length from existing metadata, was 3600000, overriding to 86400000   ".split(",")) {
            exceptions.keySet().removeIf(e -> e.message.contains(msg));
        }
        if (Jvm.hasException(exceptions)) {
            final String msg = exceptions.size() + " exceptions were detected: " + exceptions.keySet().stream().map(ek -> ek.message).collect(Collectors.joining(", "));
            Jvm.dumpException(exceptions);
            Jvm.resetExceptionHandlers();
            throw new AssertionError(msg);
        }
    }

    @After
    public void afterChecks() {
        preAfter();
        SystemTimeProvider.CLOCK = SystemTimeProvider.INSTANCE;
        CleaningThread.performCleanup(Thread.currentThread());

        // find any discarded resources.
        System.gc();
        AbstractCloseable.waitForCloseablesToClose(1000);

        if (finishedNormally) {
            assertReferencesReleased();
            checkThreadDump();
            checkExceptions();
        }

        tearDown();
    }

    protected void preAfter() {
    }

    protected void tearDown() {
    }
}
