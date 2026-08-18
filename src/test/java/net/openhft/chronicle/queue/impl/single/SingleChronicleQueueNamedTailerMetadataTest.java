/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.NavigableMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class SingleChronicleQueueNamedTailerMetadataTest extends QueueTestCommon {

    private final SetTimeProvider timeProvider = new SetTimeProvider();

    @Before
    public void useDeterministicSystemTimeProvider() {
        SystemTimeProvider.CLOCK = timeProvider;
    }

    @After
    public void resetSystemTimeProvider() {
        SystemTimeProvider.CLOCK = SystemTimeProvider.INSTANCE;
    }

    private static SingleChronicleQueueBuilder builder(File dir) {
        return SingleChronicleQueueBuilder.single(dir)
                .rollCycle(TestRollCycles.TEST_DAILY)
                .timeProvider(SystemTimeProvider.CLOCK);
    }

    private void writeDailyExcerpts(File dir, int days) {
        try (ChronicleQueue queue = builder(dir).build();
             ExcerptAppender appender = queue.createAppender()) {
            int firstCycle = Integer.MIN_VALUE;
            for (int d = 0; d < days; d++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("n").int32(d);
                }
                int cycle = queue.rollCycle().toCycle(appender.lastIndexAppended());
                if (d == 0)
                    firstCycle = cycle;
                assertEquals("each simulated day must use a distinct roll cycle",
                        firstCycle + d, cycle);
                timeProvider.advanceMillis(TimeUnit.DAYS.toMillis(1));
            }
        }
    }

    @Test
    public void namedTailerIndexesReturnsCommittedTailerPositionsOnly() {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(10_000));
        writeDailyExcerpts(dir, 2);

        try (SingleChronicleQueue q = builder(dir).build()) {
            long gatewayIndex = q.rollCycle().toIndex(q.firstCycle(), 0);
            try (ExcerptTailer gateway = q.createTailer("gateway")) {
                assertTrue(gateway.moveToIndex(gatewayIndex));
            }

            String replicated = SingleChronicleQueue.REPLICATED_NAMED_TAILER_PREFIX + "sink";
            try (ExcerptTailer sink = q.createTailer(replicated)) {
                assertTrue(sink.moveToIndex(q.rollCycle().toIndex(q.lastCycle(), 0)));
            }

            NavigableMap<String, Long> indexes = q.namedTailerIndexes();
            assertEquals(Long.valueOf(gatewayIndex), indexes.get("gateway"));
            assertEquals(Long.valueOf(q.rollCycle().toIndex(q.lastCycle(), 0)), indexes.get(replicated));
            assertTrue(indexes.keySet().stream().noneMatch(k -> k.endsWith(".lock") || k.endsWith(".version")));
        }
    }

    @Test
    public void namedTailerIndexesRetainsDistinguishableLegacyReservedIds() {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(12_000));
        writeDailyExcerpts(dir, 1);

        try (SingleChronicleQueue q = builder(dir).build()) {
            long index = q.firstIndex();
            String replicated = SingleChronicleQueue.REPLICATED_NAMED_TAILER_PREFIX + "sink";
            try (ExcerptTailer tailer = q.createTailer(replicated)) {
                assertEquals(0, tailer.index());
            }

            q.tableStorePut("index.gateway.LOCK", index);
            q.tableStorePut("index." + replicated + ".lock", index);
            q.tableStorePut("index." + replicated + ".LOCK.lock", Long.MIN_VALUE);
            q.tableStorePut("index." + replicated + ".LOCK.version", 1);
            q.tableStorePut("index." + replicated + ".Version", index);
            q.tableStorePut("index." + replicated + ".Version.lock", Long.MIN_VALUE);
            q.tableStorePut("index." + replicated + ".Version.version", 1);

            expectException("Legacy named tailer id 'gateway.LOCK'");
            expectException("Legacy named tailer id '" + replicated + ".LOCK'");
            expectException("Legacy named tailer id '" + replicated + ".Version'");
            NavigableMap<String, Long> indexes = q.namedTailerIndexes();

            assertEquals(Long.valueOf(index), indexes.get("gateway.LOCK"));
            assertEquals(Long.valueOf(index), indexes.get(replicated + ".LOCK"));
            assertEquals(Long.valueOf(index), indexes.get(replicated + ".Version"));
            assertFalse(indexes.containsKey(replicated + ".LOCK.lock"));
            assertFalse(indexes.containsKey(replicated + ".LOCK.version"));
            assertFalse(indexes.containsKey(replicated + ".Version.lock"));
            assertFalse(indexes.containsKey(replicated + ".Version.version"));
        }
    }

    @Test
    public void namedTailerIndexesReturnsDetachedSnapshot() {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(15_000));
        writeDailyExcerpts(dir, 2);

        try (SingleChronicleQueue q = builder(dir).build();
             ExcerptTailer existing = q.createTailer("existing")) {
            long firstIndex = q.rollCycle().toIndex(q.firstCycle(), 0);
            long lastIndex = q.rollCycle().toIndex(q.lastCycle(), 0);
            assertTrue(existing.moveToIndex(firstIndex));

            NavigableMap<String, Long> snapshot = q.namedTailerIndexes();

            assertTrue(existing.moveToIndex(lastIndex));
            try (ExcerptTailer later = q.createTailer("later")) {
                assertTrue(later.moveToIndex(lastIndex));
            }

            assertEquals(Long.valueOf(firstIndex), snapshot.get("existing"));
            assertFalse(snapshot.containsKey("later"));

            NavigableMap<String, Long> current = q.namedTailerIndexes();
            assertEquals(Long.valueOf(lastIndex), current.get("existing"));
            assertEquals(Long.valueOf(lastIndex), current.get("later"));
        }
    }

    @Test
    public void parkNamedTailerResetsExistingNonReplicatedTailer() {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(20_000));
        writeDailyExcerpts(dir, 3);

        try (SingleChronicleQueue q = builder(dir).build()) {
            long pinned = q.rollCycle().toIndex(q.firstCycle(), 0);
            try (ExcerptTailer dead = q.createTailer("dead")) {
                assertTrue(dead.moveToIndex(pinned));
            }

            assertEquals(NamedTailerParkResult.PARKED, q.parkNamedTailer("dead"));
            assertEquals(Long.valueOf(0L), q.namedTailerIndexes().get("dead"));
        }
    }

    @Test
    public void parkNamedTailerDoesNotCreateMissingTailer() {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(30_000));
        writeDailyExcerpts(dir, 1);

        try (SingleChronicleQueue q = builder(dir).build()) {
            assertEquals(NamedTailerParkResult.NOT_FOUND, q.parkNamedTailer("missing"));
            assertEquals(NamedTailerParkResult.INVALID_NAME, q.parkNamedTailer(null));
            assertFalse(q.namedTailerIndexes().containsKey("missing"));
        }
    }

    @Test
    public void parkNamedTailerRejectsReservedSuffixesWithoutMutatingMetadata() {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(40_000));
        writeDailyExcerpts(dir, 1);

        try (SingleChronicleQueue q = builder(dir).build();
             LongValue version = q.indexVersionForId("gateway");
             TableStoreWriteLock lock = q.versionIndexLockForId("gateway")) {
            version.setValue(42L);
            lock.lock();
            try {
                long lockBefore = q.tableStoreGet("index.gateway.lock");

                assertEquals(NamedTailerParkResult.INVALID_NAME, q.parkNamedTailer("gateway.version"));
                assertEquals(42L, version.getValue());

                assertEquals(NamedTailerParkResult.INVALID_NAME, q.parkNamedTailer("gateway.lock"));
                assertEquals(NamedTailerParkResult.INVALID_NAME, q.parkNamedTailer("gateway.LOCK"));
                assertEquals(lockBefore, q.tableStoreGet("index.gateway.lock"));
            } finally {
                lock.unlock();
            }
        }
    }

    @Test
    public void replicatedNamedTailersCannotBeParked() {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(50_000));
        writeDailyExcerpts(dir, 2);

        try (SingleChronicleQueue q = builder(dir).build()) {
            String name = SingleChronicleQueue.REPLICATED_NAMED_TAILER_PREFIX + "sink";
            long pinned = q.rollCycle().toIndex(q.firstCycle(), 0);
            try (ExcerptTailer sink = q.createTailer(name)) {
                assertTrue(sink.moveToIndex(pinned));
            }
            try (LongValue version = q.indexVersionForId(name)) {
                long versionBefore = version.getValue();

                assertEquals(NamedTailerParkResult.REFUSED_REPLICATED, q.parkNamedTailer(name));
                assertEquals(Long.valueOf(pinned), q.namedTailerIndexes().get(name));
                assertEquals(versionBefore, version.getValue());
            }
        }
    }

    @Test
    public void createTailerRejectsLockAndVersionSuffixes() {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(60_000));
        writeDailyExcerpts(dir, 1);

        try (SingleChronicleQueue q = builder(dir).build()) {
            for (String name : new String[]{"gateway.lock", "gateway.LOCK", "gateway.version", "gateway.Version"})
                assertThrows(IllegalArgumentException.class, () -> q.createTailer(name));
        }
    }

    @Test
    public void fileForCycleReturnsExistingRollFileOnly() {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(70_000));
        writeDailyExcerpts(dir, 1);

        try (SingleChronicleQueue q = builder(dir).build()) {
            File first = q.fileForCycle(q.firstCycle());
            assertNotNull(first);
            assertTrue(first.exists());
            assertNull(q.fileForCycle(q.firstCycle() - 1));
        }
    }

    @Test
    public void namedTailerMetadataApisRejectReservedSuffixes() {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(62_000));
        writeDailyExcerpts(dir, 1);

        try (SingleChronicleQueue q = builder(dir).build()) {
            assertThrows(IllegalArgumentException.class, () -> q.indexForId("gateway.LOCK"));
            assertThrows(IllegalArgumentException.class, () -> q.indexVersionForId("gateway.version"));
            assertThrows(IllegalArgumentException.class, () -> q.versionIndexLockForId("gateway.Version"));
        }
    }

    @Test
    public void namedTailerRegistrationWaitsForExclusiveMetadataLock() throws Exception {
        assertNamedTailerRegistrationWaitsForMetadataLock(false);
    }

    @Test
    public void namedTailerRegistrationWaitsForSharedMetadataLock() throws Exception {
        assertNamedTailerRegistrationWaitsForMetadataLock(true);
    }

    @Test
    public void namedTailerIndexesSupportsConcurrentRegistration() throws Exception {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(68_000));
        writeDailyExcerpts(dir, 1);

        try (SingleChronicleQueue registeringQueue = builder(dir).build();
             SingleChronicleQueue scanningQueue = builder(dir).build()) {
            ExecutorService executor = Executors.newFixedThreadPool(2);
            CountDownLatch start = new CountDownLatch(1);
            try {
                Future<?> registrations = executor.submit(() -> {
                    await(start);
                    for (int i = 0; i < 32; i++) {
                        try (ExcerptTailer tailer = registeringQueue.createTailer("concurrent-" + i)) {
                            assertEquals(0, tailer.index());
                        }
                    }
                });
                Future<?> scans = executor.submit(() -> {
                    await(start);
                    for (int i = 0; i < 64; i++)
                        scanningQueue.namedTailerIndexes();
                });

                start.countDown();
                registrations.get(30, TimeUnit.SECONDS);
                scans.get(30, TimeUnit.SECONDS);
            } finally {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }

            NavigableMap<String, Long> indexes = scanningQueue.namedTailerIndexes();
            for (int i = 0; i < 32; i++)
                assertEquals(Long.valueOf(0), indexes.get("concurrent-" + i));
        }
    }

    @Test
    public void parkedNamedTailerRemainsParkedAfterQueueRestart() {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(72_000));
        writeDailyExcerpts(dir, 3);

        try (SingleChronicleQueue q = builder(dir).build()) {
            long firstIndex = q.rollCycle().toIndex(q.firstCycle(), 0);
            try (ExcerptTailer tailer = q.createTailer("parked")) {
                assertTrue(tailer.moveToIndex(firstIndex));
            }
            assertEquals(NamedTailerParkResult.PARKED, q.parkNamedTailer("parked"));
        }

        try (SingleChronicleQueue q = builder(dir).build();
             ExcerptTailer tailer = q.createTailer("parked")) {
            assertEquals(0, tailer.index());
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private void assertNamedTailerRegistrationWaitsForMetadataLock(boolean shared) throws Exception {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(shared ? 66_000 : 65_000));
        writeDailyExcerpts(dir, 1);

        File metadataFile = new File(dir, SingleChronicleQueue.QUEUE_METADATA_FILE);
        CountDownLatch lockAcquired = new CountDownLatch(1);
        CountDownLatch releaseLock = new CountDownLatch(1);
        CountDownLatch registrationStarted = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (SingleChronicleQueue queue = builder(dir).build()) {
            Future<?> lockHolder = executor.submit(() -> {
                if (shared)
                    return SingleTableStore.doWithSharedLock(metadataFile,
                            ignored -> holdLock(lockAcquired, releaseLock), Object::new);
                return SingleTableStore.doWithExclusiveLock(metadataFile,
                        ignored -> holdLock(lockAcquired, releaseLock), Object::new);
            });
            assertTrue(lockAcquired.await(5, TimeUnit.SECONDS));

            Future<?> registration = executor.submit(() -> {
                registrationStarted.countDown();
                try (ExcerptTailer tailer = queue.createTailer("blocked-registration")) {
                    assertEquals(0, tailer.index());
                }
            });
            assertTrue(registrationStarted.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> registration.get(100, TimeUnit.MILLISECONDS));

            releaseLock.countDown();
            registration.get(5, TimeUnit.SECONDS);
            lockHolder.get(5, TimeUnit.SECONDS);
            assertTrue(queue.namedTailerIndexes().containsKey("blocked-registration"));
        } finally {
            releaseLock.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static Object holdLock(CountDownLatch lockAcquired, CountDownLatch releaseLock) {
        lockAcquired.countDown();
        await(releaseLock);
        return null;
    }
}
