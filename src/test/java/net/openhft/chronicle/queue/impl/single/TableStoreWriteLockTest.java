/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.threads.InterruptedRuntimeException;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.testframework.process.JavaProcessBuilder;
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

public class TableStoreWriteLockTest extends QueueTestCommon {

    private static final String TEST_LOCK_NAME = "testLock";
    private static final long TIMEOUT_MS = 100;
    private TableStore<Metadata.NoMeta> tableStore;
    private Path tempDir;

    @Before
    public void setUp() {
        tempDir = IOTools.createTempDirectory("namedTableStoreLockTest");
        tempDir.toFile().mkdirs();
        Path storeDirectory = tempDir.resolve("test_store.cq4t");
        tableStore = SingleTableBuilder.binary(storeDirectory, Metadata.NoMeta.INSTANCE).build();
    }

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @After
    public void tearDown() {
        Closeable.closeQuietly(tableStore);
        IOTools.deleteDirWithFiles(tempDir.toFile());
    }

    @Test(timeout = 5_000)
    public void lockWillThrowIllegalStateExceptionIfInterruptedWhileWaitingForLock() throws InterruptedException {
        try (final TableStoreWriteLock testLock = createTestLock(tableStore, 5_000)) {
            testLock.lock();
            AtomicBoolean threwException = new AtomicBoolean(false);
            Thread t = new Thread(() -> {
                try {
                    testLock.lock();
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
        try (final TableStoreWriteLock testLock = createTestLock()) {
            testLock.lock();
            assertTrue(testLock.isLockedByCurrentProcess(actualPid::set));
            assertEquals(-1, actualPid.get());
            testLock.unlock();
            assertFalse(testLock.isLockedByCurrentProcess(actualPid::set));
            assertEquals(TableStoreWriteLock.UNLOCKED, actualPid.get());
        }
    }

    @Test(timeout = 5_000)
    public void lockWillBeAcquiredAfterTimeoutWithAWarning() throws InterruptedException {
        System.setProperty("queue.force.unlock.mode", "ALWAYS");
        try (final TableStoreWriteLock testLock = createTestLock(tableStore, 50)) {
            Thread t = new Thread(testLock::lock);
            t.start();
            t.join();
            testLock.lock();
            expectException("Unlocking forcibly");
            expectException("Forced unlock");
        } finally {
            System.clearProperty("queue.force.unlock.mode");
        }
    }

    @Test(timeout = 5_000, expected = UnrecoverableTimeoutException.class)
    public void lockWillThrowExceptionAfterTimeoutWhenDontRecoverLockTimeoutIsTrue() throws InterruptedException {
        System.setProperty("queue.force.unlock.mode", "NEVER");
        try (final TableStoreWriteLock testLock = createTestLock(tableStore, 50)) {
            Thread t = new Thread(testLock::lock);
            t.start();
            t.join();
            testLock.lock();
            fail("Should have thrown trying to lock()");
        } finally {
            System.clearProperty("queue.force.unlock.mode");
        }
    }

    @Test(timeout = 5_000, expected = UnrecoverableTimeoutException.class)
    public void lockWillThrowExceptionAfterTimeoutWhenOnlyUnlockIfProcessDeadIsTrue() throws InterruptedException {
        System.setProperty("queue.force.unlock.mode", "LOCKING_PROCESS_DEAD");
        try (final TableStoreWriteLock testLock = createTestLock(tableStore, 50)) {
            Thread t = new Thread(testLock::lock);
            t.start();
            t.join();
            testLock.lock();
            fail("Should have thrown trying to lock()");
        } finally {
            System.clearProperty("queue.force.unlock.mode");
        }
    }

    @Test(timeout = 5_000)
    public void unlockWillWarnIfNotLocked() {
        try (final TableStoreWriteLock testLock = createTestLock()) {
            testLock.unlock();
            expectException("Write lock was already unlocked.");
        }
    }

    @Test(timeout = 5_000)
    public void unlockWillNotUnlockAndWarnIfLockedByAnotherProcess() throws IOException, InterruptedException, TimeoutException {
        try (final TableStoreWriteLock testLock = createTestLock()) {
            final Process process = runLockingProcess(true);
            waitForLockToBecomeLocked(testLock);
            testLock.unlock();
            assertTrue(testLock.locked());
            expectException("Write lock was locked by someone else!");
            process.destroy();
            process.waitFor();
        }
    }

    @Test(timeout = 5_000)
    public void forceUnlockWillUnlockAndWarnIfLockedByAnotherProcess() throws IOException, InterruptedException, TimeoutException {
        try (final TableStoreWriteLock testLock = createTestLock()) {
            final Process process = runLockingProcess(true);
            waitForLockToBecomeLocked(testLock);
            testLock.forceUnlock();
            assertFalse(testLock.locked());
            expectException("Forced unlock for the lock");
            process.destroy();
            process.waitFor();
        }
    }

    @Test(timeout = 5_000)
    public void forceUnlockWillNotWarnIfLockIsNotLocked() {
        try (final TableStoreWriteLock testLock = createTestLock()) {
            testLock.forceUnlock();
            assertFalse(testLock.locked());
        }
    }

    @Test(timeout = 5_000)
    public void forceUnlockWillWarnIfLockIsLockedByCurrentProcess() {
        try (final TableStoreWriteLock testLock = createTestLock()) {
            testLock.lock();
            testLock.forceUnlock();
            assertFalse(testLock.locked());
            expectException("Forced unlock for the lock");
        }
    }

    @Test(timeout = 15_000)
    public void lockPreventsConcurrentAcquisition() {
        AtomicBoolean lockIsAcquired = new AtomicBoolean(false);
        try (final TableStoreWriteLock testLock = createTestLock(tableStore, 10_000)) {
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
        assertTrue(true); // if we got here without an exception, the test passes
    }

    @Test(timeout = 5_000)
    public void forceUnlockIfProcessIsDeadWillFailWhenLockingProcessIsAlive() throws IOException, TimeoutException, InterruptedException {
        Process lockingProcess = runLockingProcess(true);
        try (TableStoreWriteLock lock = createTestLock()) {
            waitForLockToBecomeLocked(lock);
            assertFalse(lock.forceUnlockIfProcessIsDead());
            assertTrue(lock.locked());
        }
        lockingProcess.destroy();
        lockingProcess.waitFor(3_000, TimeUnit.SECONDS);
    }

    @Test(timeout = 5_000)
    public void forceUnlockIfProcessIsDeadWillSucceedWhenLockingProcessIsDead() throws IOException, TimeoutException, InterruptedException {
        ignoreException("Forced unlock");
        Process lockingProcess = runLockingProcess(false);
        try (TableStoreWriteLock lock = createTestLock()) {
            waitForLockToBecomeLocked(lock);
            lockingProcess.destroy();
            lockingProcess.waitFor(3_000, TimeUnit.SECONDS);
            assertTrue(lock.forceUnlockIfProcessIsDead());
            assertFalse(lock.locked());
        }
    }

    @Test(timeout = 5_000)
    public void forceUnlockIfProcessIsDeadWillSucceedWhenLockIsNotLocked() {
        try (TableStoreWriteLock lock = createTestLock()) {
            assertTrue(lock.forceUnlockIfProcessIsDead());
            assertFalse(lock.locked());
        }
    }

    private void waitForLockToBecomeLocked(TableStoreWriteLock lock) throws TimeoutException {
        Pauser p = Pauser.balanced();
        while (!lock.locked()) {
            p.pause(3_000, TimeUnit.SECONDS);
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedRuntimeException("Interrupted waiting for lock to lock");
            }
        }
    }

    private TableStoreWriteLock createTestLock() {
        return createTestLock(tableStore, TIMEOUT_MS);
    }

    @NotNull
    private static TableStoreWriteLock createTestLock(TableStore<Metadata.NoMeta> tableStore, long timeoutMilliseconds) {
        return new TableStoreWriteLock(tableStore, Pauser::balanced, timeoutMilliseconds, TEST_LOCK_NAME);
    }

    private Process runLockingProcess(boolean releaseAfterInterrupt) {
        return JavaProcessBuilder.create(LockAndHoldUntilInterrupted.class)
                .withProgramArguments(tableStore.file().getAbsolutePath(), String.valueOf(releaseAfterInterrupt)).start();
    }

    private static void lockAndHoldUntilInterrupted(String tableStorePath, boolean releaseWhenInterrupted) {
        try (TableStore<Metadata.NoMeta> tableStore = SingleTableBuilder.binary(tableStorePath, Metadata.NoMeta.INSTANCE).build();
             TableStoreWriteLock lock = createTestLock(tableStore, 15_000)) {
            lock.lock();
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

        private final TableStoreWriteLock tableStoreWriteLock;
        private final AtomicBoolean lockIsAcquired;
        private final int numberOfIterations;
        private final CyclicBarrier barrier;

        LockAcquirer(TableStoreWriteLock tableStoreWriteLock, AtomicBoolean lockIsAcquired, int numberOfIterations, CyclicBarrier barrier) {
            this.tableStoreWriteLock = tableStoreWriteLock;
            this.lockIsAcquired = lockIsAcquired;
            this.numberOfIterations = numberOfIterations;
            this.barrier = barrier;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                for (int i = 0; i < numberOfIterations; i++) {
                    tableStoreWriteLock.lock();
                    try {
                        lockIsAcquired.compareAndSet(false, true);
                        Jvm.pause(10);
                        lockIsAcquired.compareAndSet(true, false);
                    } finally {
                        tableStoreWriteLock.unlock();
                        Jvm.pause(1);
                    }
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
    }
}
