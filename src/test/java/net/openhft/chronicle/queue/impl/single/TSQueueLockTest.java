package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.onoes.LogLevel;
import net.openhft.chronicle.core.threads.InterruptedRuntimeException;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.testframework.process.ProcessRunner;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.Threads;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class TSQueueLockTest extends QueueTestCommon {

    private static final String TEST_LOCK_NAME = "testLock";
    private static final long TIMEOUT_MS = 100;
    private TableStore<Metadata.NoMeta> tableStore;

    @Before
    public void setUp() {
        final Path tempDir = IOTools.createTempDirectory(this.getClass().getSimpleName());
        tempDir.toFile().mkdirs();
        Path storeDirectory = tempDir.resolve("test_store.cq4t");
        tableStore = SingleTableBuilder.binary(storeDirectory, Metadata.NoMeta.INSTANCE).build();
    }

    @After
    public void tearDown() {
        Closeable.closeQuietly(tableStore);
    }

    @Test(timeout = 5_000)
    public void lockWillThrowIllegalStateExceptionIfInterruptedWhileWaitingForLock() throws InterruptedException {
        try (final TSQueueLock testLock = createTestLock(tableStore, 5_000)) {
            testLock.acquireLock();
            AtomicBoolean threwException = new AtomicBoolean(false);
            Thread t = new Thread(() -> {
                try {
                    testLock.acquireLock();
                } catch (IllegalStateException e) {
                    threwException.set(true);
                }
            });
            t.start();
            Jvm.pause(10);
            t.interrupt();
            t.join();
            assertTrue(threwException.get());
        }
    }

    @Test(timeout = 5_000)
    public void testIsLockedByCurrentProcess() {
        AtomicLong actualPid = new AtomicLong(-1);
        try (final TSQueueLock testLock = createTestLock()) {
            testLock.acquireLock();
            assertTrue(testLock.isLockedByCurrentProcess(actualPid::set));
            assertEquals(-1, actualPid.get());
            testLock.unlock();
            assertFalse(testLock.isLockedByCurrentProcess(actualPid::set));
            assertEquals(TSQueueLock.UNLOCKED, actualPid.get());
        }
    }

    @Test(timeout = 5_000)
    public void lockWillBeAcquiredAfterTimeoutWithAWarning() throws InterruptedException {
        try (final TSQueueLock testLock = createTestLock(tableStore, 50)) {
            Thread t = new Thread(testLock::acquireLock);
            t.start();
            t.join();
            testLock.acquireLock();
            assertTrue(exceptions.keySet().stream()
                    .anyMatch(ek -> ek.level == LogLevel.WARN && ek.clazz == TSQueueLock.class && ek.message.startsWith("Forced unlock")));
            expectException("Unlocking forcibly");
            expectException("Forced unlock");
        }
    }

    @Test(timeout = 5_000, expected = UnrecoverableTimeoutException.class)
    public void lockWillThrowExceptionAfterTimeoutWhenDontRecoverLockTimeoutIsTrue() throws InterruptedException {
        expectException("queue.dont.recover.lock.timeout property is deprecated and will be removed");
        System.setProperty("queue.dont.recover.lock.timeout", "true");
        try (final TSQueueLock testLock = createTestLock(tableStore, 50)) {
            Thread t = new Thread(testLock::acquireLock);
            t.start();
            t.join();
            testLock.acquireLock();
            fail("Should have thrown trying to lock()");
        } finally {
            System.clearProperty("queue.dont.recover.lock.timeout");
        }
    }

    @Test(timeout = 5_000, expected = UnrecoverableTimeoutException.class)
    public void lockWillThrowExceptionAfterTimeoutWhenOnlyUnlockIfProcessDeadIsTrue() throws InterruptedException {
        System.setProperty("queue.force.unlock.mode", "LOCKING_PROCESS_DEAD");
        expectException("Couldn't acquire lock after");
        try (final TSQueueLock testLock = createTestLock(tableStore, 50)) {
            Thread t = new Thread(testLock::acquireLock);
            t.start();
            t.join();
            testLock.acquireLock();
            fail("Should have thrown trying to lock()");
        } finally {
            System.clearProperty("queue.force.unlock.mode");
        }
    }

    @Test(timeout = 5_000)
    public void unlockWillWarnIfNotLocked() {
        try (final TSQueueLock testLock = createTestLock()) {
            testLock.unlock();
            assertTrue(exceptions.keySet().stream()
                    .anyMatch(ek -> ek.level == LogLevel.WARN && ek.clazz == TSQueueLock.class && ek.message.startsWith("Queue lock was locked by another thread")));
            expectException("Queue lock was locked by another thread");
        }
    }

    @Test(timeout = 5_000)
    public void unlockWillNotUnlockAndWarnIfLockedByAnotherProcess() throws IOException, InterruptedException, TimeoutException {
        try (final TSQueueLock testLock = createTestLock()) {
            final Process process = runLockingProcess(true);
            waitForLockToBecomeLocked(testLock);
            testLock.unlock();
            assertTrue(testLock.isLocked());
            assertTrue(exceptions.keySet().stream()
                    .anyMatch(ek -> ek.level == LogLevel.WARN && ek.clazz == TSQueueLock.class && ek.message.startsWith("Queue lock was locked by another thread")));
            expectException("Queue lock was locked by another thread");
            process.destroy();
            process.waitFor();
        }
    }

    @Test(timeout = 15_000)
    public void lockPreventsConcurrentAcquisition() {
        AtomicBoolean lockIsAcquired = new AtomicBoolean(false);
        try (final TSQueueLock testLock = createTestLock(tableStore, 10_000)) {
            int numThreads = Math.min(6, Runtime.getRuntime().availableProcessors());
            ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            final Collection<Future<?>> futures = IntStream.range(0, numThreads)
                    .mapToObj(v -> executorService.submit(new LockAcquirer(testLock, lockIsAcquired, 30, barrier)))
                    .collect(Collectors.toList());
            futures.forEach(fut -> {
                try {
                    fut.get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Threads.shutdown(executorService);
        }
    }

    @Test(timeout = 5_000)
    public void forceUnlockIfProcessIsDeadWillFailWhenLockingProcessIsAlive() throws IOException, TimeoutException, InterruptedException {
        Process lockingProcess = runLockingProcess(true);
        try (TSQueueLock lock = createTestLock()) {
            waitForLockToBecomeLocked(lock);
            assertFalse(lock.forceUnlockIfProcessIsDead());
            assertTrue(lock.isLocked());
        }
        lockingProcess.destroy();
        lockingProcess.waitFor(5_000, TimeUnit.SECONDS);
    }

    @Test(timeout = 5_000)
    public void forceUnlockIfProcessIsDeadWillSucceedWhenLockingProcessIsDead() throws IOException, TimeoutException, InterruptedException {
        Process lockingProcess = runLockingProcess(false);
        try (TSQueueLock lock = createTestLock()) {
            waitForLockToBecomeLocked(lock);
            lockingProcess.destroy();
            lockingProcess.waitFor(5_000, TimeUnit.SECONDS);
            assertTrue(lock.forceUnlockIfProcessIsDead());
            assertFalse(lock.isLocked());
        }
    }

    @Test(timeout = 5_000)
    public void forceUnlockIfProcessIsDeadWillSucceedWhenLockIsNotLocked() {
        try (TSQueueLock lock = createTestLock()) {
            assertTrue(lock.forceUnlockIfProcessIsDead());
            assertFalse(lock.isLocked());
        }
    }

    @Test
    public void forceUnlockOnBehalfOfThreadDoesNothingWhenNotLocked() {
        try (TSQueueLock lock = createTestLock()) {
            lock.forceUnlockOnBehalfOfThread(12345L);
            assertFalse(lock.isLocked());
        }
    }

    @Test
    public void forceUnlockOnBehalfOfThreadWillSucceedWithMatchingThreadID() throws TimeoutException, InterruptedException {
        try (TSQueueLock lock = createTestLock()) {
            Thread t = new Thread(lock::acquireLock);
            t.start();
            waitForLockToBecomeLocked(lock);
            lock.forceUnlockOnBehalfOfThread(t.getId());
            assertFalse(lock.isLocked());
            t.join();
        }
    }

    @Test
    public void forceUnlockOnBehalfOfThreadWillFailWhenThreadIDDoesNotMatch() throws TimeoutException {
        expectException("Queue lock was locked by another thread, provided-tid");
        try (TSQueueLock lock = createTestLock()) {
            lock.acquireLock();
            waitForLockToBecomeLocked(lock);
            lock.forceUnlockOnBehalfOfThread(Thread.currentThread().getId() + 1);
            assertTrue(lock.isLocked());
        }
    }

    private void waitForLockToBecomeLocked(TSQueueLock lock) throws TimeoutException {
        Pauser p = Pauser.balanced();
        while (!lock.isLocked()) {
            p.pause(5_000, TimeUnit.SECONDS);
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedRuntimeException("Interrupted waiting for lock to lock");
            }
        }
    }

    private TSQueueLock createTestLock() {
        return createTestLock(tableStore, TIMEOUT_MS);
    }

    @NotNull
    private static TSQueueLock createTestLock(TableStore<Metadata.NoMeta> tableStore, long timeoutMilliseconds) {
        return new TSQueueLock(tableStore, Pauser::balanced, timeoutMilliseconds);
    }

    private Process runLockingProcess(boolean releaseAfterInterrupt) throws IOException {
        return ProcessRunner.runClass(LockAndHoldUntilInterrupted.class,
                tableStore.file().getAbsolutePath(), String.valueOf(releaseAfterInterrupt));
    }

    private static void lockAndHoldUntilInterrupted(String tableStorePath, boolean releaseWhenInterrupted) {
        try (TableStore<Metadata.NoMeta> tableStore = SingleTableBuilder.binary(tableStorePath, Metadata.NoMeta.INSTANCE).build();
             TSQueueLock lock = createTestLock(tableStore, 15_000)) {
            lock.acquireLock();
            while (!Thread.currentThread().isInterrupted()) {
                Jvm.pause(100);
            }
            if (releaseWhenInterrupted) {
                lock.unlock();
            }
        }
    }

    static class LockAndHoldUntilInterrupted {

        public static void main(String[] args) {
            lockAndHoldUntilInterrupted(args[0], Boolean.parseBoolean(args[1]));
        }
    }

    static class LockAcquirer implements Runnable {

        private final TSQueueLock TSQueueLock;
        private final AtomicBoolean lockIsAcquired;
        private final int numberOfIterations;
        private final CyclicBarrier barrier;

        LockAcquirer(TSQueueLock TSQueueLock, AtomicBoolean lockIsAcquired, int numberOfIterations, CyclicBarrier barrier) {
            this.TSQueueLock = TSQueueLock;
            this.lockIsAcquired = lockIsAcquired;
            this.numberOfIterations = numberOfIterations;
            this.barrier = barrier;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                for (int i = 0; i < numberOfIterations; i++) {
                    TSQueueLock.acquireLock();
                    try {
                        lockIsAcquired.compareAndSet(false, true);
                        Jvm.pause(10);
                        lockIsAcquired.compareAndSet(true, false);
                    } finally {
                        TSQueueLock.unlock();
                        Jvm.pause(1);
                    }
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
    }
}