/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.testframework.ExecutorServiceUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SingleTableStoreLockTest extends QueueTestCommon {

    private static final int LOCK_SIZE = 1;
    private static final long LOCK_START = Long.MAX_VALUE - LOCK_SIZE;

    @Before
    public void trackTestThreads() {
        threadDump();
    }

    @Test
    public void exclusiveBodyRunsOnceAndPreservesRuntimeExceptionAndErrorIdentity() throws IOException {
        assertBodyRunsOnceAndPreservesFailureIdentity(false);
    }

    @Test
    public void sharedBodyRunsOnceAndPreservesRuntimeExceptionAndErrorIdentity() throws IOException {
        assertBodyRunsOnceAndPreservesFailureIdentity(true);
    }

    @Test
    public void exclusiveContentionIsRetriedBeforeBodyRunsOnce() throws Exception {
        assertContentionIsRetriedBeforeBodyRunsOnce(false);
    }

    @Test
    public void sharedContentionIsRetriedBeforeBodyRunsOnce() throws Exception {
        assertContentionIsRetriedBeforeBodyRunsOnce(true);
    }

    @Test
    public void subprocessContentionRetriesNullBeforeBody() throws Exception {
        final File file = newLockFile();
        final String java = new File(new File(System.getProperty("java.home"), "bin"), "java").getPath();
        final Process holder = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                LockHolder.class.getName(), file.getAbsolutePath()).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicInteger bodyCalls = new AtomicInteger();
        final AtomicInteger targetCalls = new AtomicInteger();
        final BufferedReader output = new BufferedReader(new InputStreamReader(holder.getInputStream(), StandardCharsets.UTF_8));
        try {
            assertEquals("LOCKED", executor.submit(output::readLine).get(15, TimeUnit.SECONDS));
            final ObservedFileLockRuntime runtime = new ObservedFileLockRuntime(file, false);
            final Future<String> result = executor.submit(() -> withLock(file, false, target -> {
                bodyCalls.incrementAndGet();
                return target;
            }, () -> {
                targetCalls.incrementAndGet();
                return "done";
            }, TimeUnit.SECONDS.toNanos(5), runtime));

            assertTrue("the real null-contention result was not observed", runtime.contention.await(15, TimeUnit.SECONDS));
            assertTrue("same-JVM overlap was observed instead of null", runtime.nullResults.get() > 0);
            assertEquals(0, bodyCalls.get());
            assertEquals(0, targetCalls.get());
            holder.getOutputStream().write(1);
            holder.getOutputStream().flush();
            assertTrue("lock-holder process did not finish", holder.waitFor(15, TimeUnit.SECONDS));
            assertEquals(0, holder.exitValue());
            assertEquals("done", result.get(15, TimeUnit.SECONDS));
            assertEquals(1, bodyCalls.get());
            assertEquals(1, targetCalls.get());
        } finally {
            holder.destroyForcibly();
            try {
                assertTrue("lock-holder process survived cleanup", holder.waitFor(15, TimeUnit.SECONDS));
            } finally {
                output.close();
                ExecutorServiceUtil.shutdownForciblyAndWaitForTermination(executor);
            }
        }
    }

    @Test
    public void scriptedNullContentionSignalsAttemptBeforeBodyAndThenSucceeds() throws Exception {
        final File file = newLockFile();
        final CountDownLatch attempted = new CountDownLatch(1);
        final CountDownLatch releaseContention = new CountDownLatch(1);
        final AtomicInteger scriptedAttempts = new AtomicInteger();
        final AtomicLong now = new AtomicLong();
        final AtomicInteger targetCalls = new AtomicInteger();
        final AtomicInteger bodyCalls = new AtomicInteger();
        final TestLockRuntime runtime = new TestLockRuntime(
                () -> {
                    attempted.countDown();
                    return scriptedAttempts.getAndIncrement() > 0;
                },
                now::get,
                ignored -> {
                    await(releaseContention);
                    now.incrementAndGet();
                });
        final ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            final Future<String> result = executor.submit(() -> withLock(file, false, target -> {
                bodyCalls.incrementAndGet();
                return target + "-done";
            }, () -> {
                targetCalls.incrementAndGet();
                return "target";
            }, 2L, runtime));

            assertTrue("lock acquisition was not attempted", attempted.await(5, TimeUnit.SECONDS));
            assertThrows("body ran before scripted contention was released", TimeoutException.class,
                    () -> result.get(100, TimeUnit.MILLISECONDS));
            assertEquals(0, targetCalls.get());
            assertEquals(0, bodyCalls.get());

            releaseContention.countDown();

            assertEquals("target-done", result.get(5, TimeUnit.SECONDS));
            assertEquals(2, runtime.attemptCalls.get());
            assertEquals(1, runtime.pauseCalls.get());
            assertEquals(1, runtime.closeLockCalls.get());
            assertEquals(1, runtime.closeCalls.get());
            assertEquals(1, targetCalls.get());
            assertEquals(1, bodyCalls.get());
        } finally {
            releaseContention.countDown();
            ExecutorServiceUtil.shutdownForciblyAndWaitForTermination(executor);
        }
    }

    @Test
    public void scriptedAcquisitionIOExceptionRetainsExactCauseAndSkipsBody() throws IOException {
        final File file = newLockFile();
        final IOException acquisitionFailure = new IOException("deliberate acquisition failure");
        final AtomicInteger targetCalls = new AtomicInteger();
        final AtomicInteger bodyCalls = new AtomicInteger();
        final TestLockRuntime runtime = new TestLockRuntime(
                () -> {
                    throw acquisitionFailure;
                },
                () -> 0L,
                ignored -> {
                });

        final IllegalStateException actual = assertThrows(IllegalStateException.class,
                () -> withLock(file, false, target -> {
                    bodyCalls.incrementAndGet();
                    return target;
                }, () -> {
                    targetCalls.incrementAndGet();
                    return "target";
                }, TimeUnit.SECONDS.toNanos(1), runtime));

        assertSame(acquisitionFailure, actual.getCause());
        assertEquals(1, runtime.attemptCalls.get());
        assertEquals(0, runtime.closeLockCalls.get());
        assertEquals(1, runtime.closeCalls.get());
        assertEquals(0, targetCalls.get());
        assertEquals(0, bodyCalls.get());
    }

    @Test
    public void scriptedTimeoutRetainsOverlapCause() throws IOException {
        final OverlappingFileLockException overlap = new OverlappingFileLockException();
        final IllegalStateException actual = assertScriptedTimeout(() -> {
            throw overlap;
        });

        assertSame(overlap, actual.getCause());
    }

    @Test
    public void scriptedNullContentionTimesOutWithoutInventingACause() throws IOException {
        final IllegalStateException actual = assertScriptedTimeout(() -> false);

        assertNull(actual.getCause());
    }

    @Test
    public void timeoutRetainsChannelCloseFailureAsSuppressed() throws IOException {
        final File file = newLockFile();
        final AtomicLong now = new AtomicLong();
        final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(1);
        final IOException channelCloseFailure = new IOException("deliberate channel close failure");
        final TestLockRuntime runtime = new TestLockRuntime(
                () -> false,
                now::get,
                ignored -> now.set(timeoutNanos),
                null,
                channelCloseFailure);

        final IllegalStateException actual = assertThrows(IllegalStateException.class,
                () -> withLock(file, false, target -> target, () -> "target", timeoutNanos, runtime));

        assertTrue(actual.getMessage().contains("Unable to claim the exclusive lock"));
        assertNull(actual.getCause());
        assertEquals(1, actual.getSuppressed().length);
        assertSame(channelCloseFailure, actual.getSuppressed()[0]);
        assertEquals(0, runtime.closeLockCalls.get());
        assertEquals(1, runtime.closeCalls.get());
    }

    @Test
    public void bodyFailureRetainsLockAndChannelCleanupFailures() throws IOException {
        final File file = newLockFile();
        final RuntimeException bodyFailure = new RuntimeException("deliberate body failure");
        final IOException lockCloseFailure = new IOException("deliberate lock close failure");
        final IOException channelCloseFailure = new IOException("deliberate channel close failure");
        final TestLockRuntime runtime = new TestLockRuntime(
                () -> true,
                () -> 0L,
                ignored -> {
                },
                lockCloseFailure,
                channelCloseFailure);

        final RuntimeException actual = assertThrows(RuntimeException.class,
                () -> withLock(file, false, ignored -> {
                    throw bodyFailure;
                }, () -> "target", TimeUnit.SECONDS.toNanos(1), runtime));

        assertSame(bodyFailure, actual);
        assertEquals(2, actual.getSuppressed().length);
        assertSame(lockCloseFailure, actual.getSuppressed()[0]);
        assertSame(channelCloseFailure, actual.getSuppressed()[1]);
        assertEquals(1, runtime.closeLockCalls.get());
        assertEquals(1, runtime.closeCalls.get());
    }

    @Test
    public void successfulBodyReportsLockCloseFailureWithChannelCloseSuppressed() throws IOException {
        final File file = newLockFile();
        final IOException lockCloseFailure = new IOException("deliberate lock close failure");
        final IOException channelCloseFailure = new IOException("deliberate channel close failure");
        final TestLockRuntime runtime = new TestLockRuntime(
                () -> true,
                () -> 0L,
                ignored -> {
                },
                lockCloseFailure,
                channelCloseFailure);

        final IllegalStateException actual = assertThrows(IllegalStateException.class,
                () -> withLock(file, false, target -> target, () -> "target",
                        TimeUnit.SECONDS.toNanos(1), runtime));

        assertSame(lockCloseFailure, actual.getCause());
        assertEquals(1, lockCloseFailure.getSuppressed().length);
        assertSame(channelCloseFailure, lockCloseFailure.getSuppressed()[0]);
        assertEquals(1, runtime.closeLockCalls.get());
        assertEquals(1, runtime.closeCalls.get());
    }

    @Test
    public void successfulBodyReportsChannelCloseFailure() throws IOException {
        final File file = newLockFile();
        final IOException channelCloseFailure = new IOException("deliberate channel close failure");
        final TestLockRuntime runtime = new TestLockRuntime(
                () -> true,
                () -> 0L,
                ignored -> {
                },
                null,
                channelCloseFailure);

        final IllegalStateException actual = assertThrows(IllegalStateException.class,
                () -> withLock(file, false, target -> target, () -> "target",
                        TimeUnit.SECONDS.toNanos(1), runtime));

        assertSame(channelCloseFailure, actual.getCause());
        assertEquals(1, runtime.closeLockCalls.get());
        assertEquals(1, runtime.closeCalls.get());
    }

    @Test
    public void publicLockMethodsPreserveCheckedSupplierAndBodyFailures() throws IOException {
        final File file = newLockFile();
        for (boolean shared : new boolean[]{false, true}) {
            final AtomicInteger supplierCalls = new AtomicInteger();
            final AtomicInteger bodyCalls = new AtomicInteger();
            final IOException supplierFailure = new IOException("checked supplier failure, shared=" + shared);

            final IOException actualSupplierFailure = assertThrows(IOException.class,
                    () -> withPublicLock(file, shared, ignored -> {
                        bodyCalls.incrementAndGet();
                        return "unused";
                    }, () -> {
                        supplierCalls.incrementAndGet();
                        throw Jvm.rethrow(supplierFailure);
                    }));
            assertSame(supplierFailure, actualSupplierFailure);
            assertEquals(1, supplierCalls.get());
            assertEquals(0, bodyCalls.get());

            supplierCalls.set(0);
            final IOException bodyFailure = new IOException("checked body failure, shared=" + shared);
            final IOException actualBodyFailure = assertThrows(IOException.class,
                    () -> withPublicLock(file, shared, ignored -> {
                        bodyCalls.incrementAndGet();
                        throw Jvm.rethrow(bodyFailure);
                    }, () -> {
                        supplierCalls.incrementAndGet();
                        return "target";
                    }));
            assertSame(bodyFailure, actualBodyFailure);
            assertEquals(1, supplierCalls.get());
            assertEquals(1, bodyCalls.get());
            assertEquals("reacquired", withPublicLock(file, shared, ignored -> "reacquired", () -> "target"));
        }
    }

    @Test
    public void sameFailureObjectIsNeverSelfSuppressed() throws IOException {
        final File file = newLockFile();
        final IOException bodyAndLockFailure = new IOException("same body and lock failure");
        final TestLockRuntime bodyRuntime = new TestLockRuntime(
                () -> true, () -> 0L, ignored -> {
                }, bodyAndLockFailure, null);

        final IOException actualBodyFailure = assertThrows(IOException.class,
                () -> withLock(file, false, ignored -> {
                    throw Jvm.rethrow(bodyAndLockFailure);
                }, () -> "target", TimeUnit.SECONDS.toNanos(1), bodyRuntime));
        assertSame(bodyAndLockFailure, actualBodyFailure);
        assertEquals(0, actualBodyFailure.getSuppressed().length);

        final IOException bodyAndChannelFailure = new IOException("same body and channel failure");
        final TestLockRuntime channelRuntime = new TestLockRuntime(
                () -> true, () -> 0L, ignored -> {
                }, null, bodyAndChannelFailure);
        final IOException actualChannelFailure = assertThrows(IOException.class,
                () -> withLock(file, false, ignored -> {
                    throw Jvm.rethrow(bodyAndChannelFailure);
                }, () -> "target", TimeUnit.SECONDS.toNanos(1), channelRuntime));
        assertSame(bodyAndChannelFailure, actualChannelFailure);
        assertEquals(0, actualChannelFailure.getSuppressed().length);

        final IOException lockAndChannelFailure = new IOException("same lock and channel failure");
        final TestLockRuntime cleanupRuntime = new TestLockRuntime(
                () -> true, () -> 0L, ignored -> {
                }, lockAndChannelFailure, lockAndChannelFailure);
        final IllegalStateException actualCleanupFailure = assertThrows(IllegalStateException.class,
                () -> withLock(file, false, ignored -> "done", () -> "target",
                        TimeUnit.SECONDS.toNanos(1), cleanupRuntime));
        assertSame(lockAndChannelFailure, actualCleanupFailure.getCause());
        assertEquals(0, lockAndChannelFailure.getSuppressed().length);
    }

    private void assertBodyRunsOnceAndPreservesFailureIdentity(boolean shared) throws IOException {
        final File file = newLockFile();
        final AtomicInteger targetCalls = new AtomicInteger();
        final AtomicInteger bodyCalls = new AtomicInteger();
        final OverlappingFileLockException expectedRuntimeException = new OverlappingFileLockException();

        final OverlappingFileLockException actualRuntimeException = assertThrows(
                OverlappingFileLockException.class,
                () -> withLock(file, shared, target -> {
                    bodyCalls.incrementAndGet();
                    throw expectedRuntimeException;
                }, () -> {
                    targetCalls.incrementAndGet();
                    return "target";
                }));

        assertSame(expectedRuntimeException, actualRuntimeException);
        assertEquals(1, targetCalls.get());
        assertEquals(1, bodyCalls.get());

        targetCalls.set(0);
        bodyCalls.set(0);
        final AssertionError expectedError = new AssertionError("deliberate body failure");

        final AssertionError actualError = assertThrows(AssertionError.class,
                () -> withLock(file, shared, target -> {
                    bodyCalls.incrementAndGet();
                    throw expectedError;
                }, () -> {
                    targetCalls.incrementAndGet();
                    return "target";
                }));

        assertSame(expectedError, actualError);
        assertEquals(1, targetCalls.get());
        assertEquals(1, bodyCalls.get());
    }

    private void assertContentionIsRetriedBeforeBodyRunsOnce(boolean shared) throws Exception {
        final File file = newLockFile();
        final AtomicInteger targetCalls = new AtomicInteger();
        final AtomicInteger bodyCalls = new AtomicInteger();
        final ExecutorService executor = Executors.newSingleThreadExecutor();

        try (FileChannel blockerChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {
            final FileLock blocker = blockerChannel.lock(LOCK_START, LOCK_SIZE, false);
            try {
                final ObservedFileLockRuntime runtime = new ObservedFileLockRuntime(file, shared);
                final Future<String> result = executor.submit(() -> {
                    return withLock(file, shared, target -> {
                        bodyCalls.incrementAndGet();
                        return target + "-done";
                    }, () -> {
                        targetCalls.incrementAndGet();
                        return "target";
                    }, TimeUnit.SECONDS.toNanos(5), runtime);
                });

                assertTrue("lock contention was not observed", runtime.contention.await(5, TimeUnit.SECONDS));
                assertThrows("body ran while the conflicting lock was still held", TimeoutException.class,
                        () -> result.get(100, TimeUnit.MILLISECONDS));
                assertEquals(0, targetCalls.get());
                assertEquals(0, bodyCalls.get());

                blocker.release();

                assertEquals("target-done", result.get(5, TimeUnit.SECONDS));
                assertEquals(1, targetCalls.get());
                assertEquals(1, bodyCalls.get());
            } finally {
                if (blocker.isValid())
                    blocker.release();
            }
        } finally {
            ExecutorServiceUtil.shutdownForciblyAndWaitForTermination(executor);
        }
    }

    private IllegalStateException assertScriptedTimeout(LockAttempt attempt) throws IOException {
        final File file = newLockFile();
        final AtomicLong now = new AtomicLong();
        final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(1);
        final AtomicInteger targetCalls = new AtomicInteger();
        final AtomicInteger bodyCalls = new AtomicInteger();
        final TestLockRuntime runtime = new TestLockRuntime(
                attempt,
                now::get,
                ignored -> now.set(timeoutNanos));

        final IllegalStateException actual = assertThrows(IllegalStateException.class,
                () -> withLock(file, false, target -> {
                    bodyCalls.incrementAndGet();
                    return target;
                }, () -> {
                    targetCalls.incrementAndGet();
                    return "target";
                }, timeoutNanos, runtime));

        assertTrue(actual.getMessage().contains("Unable to claim the exclusive lock"));
        assertEquals(1, runtime.attemptCalls.get());
        assertEquals(1, runtime.pauseCalls.get());
        assertEquals(0, runtime.closeLockCalls.get());
        assertEquals(1, runtime.closeCalls.get());
        assertEquals(0, targetCalls.get());
        assertEquals(0, bodyCalls.get());
        return actual;
    }

    private File newLockFile() throws IOException {
        final File directory = getTmpDir();
        assertTrue(directory.mkdirs() || directory.isDirectory());
        return Files.createFile(new File(directory, "table-lock" + SingleTableStore.SUFFIX).toPath()).toFile();
    }

    private <T, R> R withLock(File file,
                              boolean shared,
                              Function<T, ? extends R> code,
                              Supplier<T> target) {
        if (shared)
            return SingleTableStore.doWithSharedLock(file, code, target);
        return SingleTableStore.doWithExclusiveLock(file, code, target);
    }

    private <T, R> R withLock(File file,
                              boolean shared,
                              Function<T, ? extends R> code,
                              Supplier<T> target,
                              long timeoutNanos,
                              SingleTableStore.LockRuntime lockRuntime) {
        return SingleTableStore.doWithLock(file, code, target, shared, timeoutNanos, lockRuntime);
    }

    private <T, R> R withPublicLock(File file,
                                    boolean shared,
                                    Function<T, ? extends R> code,
                                    Supplier<T> target) {
        return shared
                ? SingleTableStore.doWithSharedLock(file, code, target)
                : SingleTableStore.doWithExclusiveLock(file, code, target);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS))
                throw new AssertionError("timed out waiting for scripted contention release");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for scripted contention release", e);
        }
    }

    public static final class LockHolder {
        public static void main(String[] args) throws IOException {
            try (FileChannel channel = FileChannel.open(new File(args[0]).toPath(), StandardOpenOption.WRITE);
                 FileLock lock = channel.lock(LOCK_START, LOCK_SIZE, false)) {
                if (!lock.isValid())
                    throw new IOException("lock acquisition did not return a valid lock");
                System.out.println("LOCKED");
                System.out.flush();
                System.in.read();
            }
        }
    }

    private static final class ObservedFileLockRuntime implements SingleTableStore.LockRuntime {
        private final SingleTableStore.FileChannelLockRuntime delegate;
        private final CountDownLatch contention = new CountDownLatch(1);
        private final AtomicInteger nullResults = new AtomicInteger();

        ObservedFileLockRuntime(File file, boolean shared) throws IOException {
            delegate = new SingleTableStore.FileChannelLockRuntime(file,
                    shared ? StandardOpenOption.READ : StandardOpenOption.WRITE, shared);
        }

        @Override
        public boolean tryLock() throws IOException {
            try {
                final boolean locked = delegate.tryLock();
                if (!locked) {
                    nullResults.incrementAndGet();
                    contention.countDown();
                }
                return locked;
            } catch (OverlappingFileLockException e) {
                contention.countDown();
                throw e;
            }
        }

        @Override
        public long nanoTime() {
            return delegate.nanoTime();
        }

        @Override
        public void pause(int millis) {
            delegate.pause(millis);
        }

        @Override
        public void closeLock() throws IOException {
            delegate.closeLock();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    @FunctionalInterface
    private interface LockAttempt {
        boolean tryLock() throws IOException;
    }

    private static final class TestLockRuntime implements SingleTableStore.LockRuntime {
        private final LockAttempt attempt;
        private final LongSupplier nanoTime;
        private final IntConsumer pause;
        private final IOException lockCloseFailure;
        private final IOException channelCloseFailure;
        private final AtomicInteger attemptCalls = new AtomicInteger();
        private final AtomicInteger pauseCalls = new AtomicInteger();
        private final AtomicInteger closeLockCalls = new AtomicInteger();
        private final AtomicInteger closeCalls = new AtomicInteger();

        private TestLockRuntime(LockAttempt attempt,
                                LongSupplier nanoTime,
                                IntConsumer pause) {
            this(attempt, nanoTime, pause, null, null);
        }

        private TestLockRuntime(LockAttempt attempt,
                                LongSupplier nanoTime,
                                IntConsumer pause,
                                IOException lockCloseFailure,
                                IOException channelCloseFailure) {
            this.attempt = attempt;
            this.nanoTime = nanoTime;
            this.pause = pause;
            this.lockCloseFailure = lockCloseFailure;
            this.channelCloseFailure = channelCloseFailure;
        }

        @Override
        public long nanoTime() {
            return nanoTime.getAsLong();
        }

        @Override
        public boolean tryLock() throws IOException {
            attemptCalls.incrementAndGet();
            return attempt.tryLock();
        }

        @Override
        public void closeLock() throws IOException {
            closeLockCalls.incrementAndGet();
            if (lockCloseFailure != null)
                throw lockCloseFailure;
        }

        @Override
        public void pause(int millis) {
            pauseCalls.incrementAndGet();
            pause.accept(millis);
        }

        @Override
        public void close() throws IOException {
            closeCalls.incrementAndGet();
            if (channelCloseFailure != null)
                throw channelCloseFailure;
        }
    }
}
