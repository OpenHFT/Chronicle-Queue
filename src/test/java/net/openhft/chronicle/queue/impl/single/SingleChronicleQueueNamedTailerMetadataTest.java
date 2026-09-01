/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class SingleChronicleQueueNamedTailerMetadataTest extends QueueTestCommon {

    private final SetTimeProvider timeProvider = new SetTimeProvider();

    private SingleChronicleQueueBuilder builder(File dir) {
        return SingleChronicleQueueBuilder.single(dir)
                .rollCycle(TestRollCycles.TEST_DAILY)
                .timeProvider(timeProvider);
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
    public void closedCachedMetadataValueIsReacquired() {
        try (SingleChronicleQueue queue = builder(getTmpDir()).build()) {
            LongValue first = queue.tableStoreAcquire("test.closed.value", 41L);
            assertNotNull(first);
            assertEquals(41L, first.getVolatileValue());
            first.close();
            assertTrue(first.isClosed());

            // Protected metadata handles can be closed by subclass/package callers; a closed
            // cached handle must not poison later access to the persisted value.
            LongValue replacement = queue.tableStoreAcquire("test.closed.value", 0L);

            assertNotSame(first, replacement);
            assertFalse(replacement.isClosed());
            assertEquals(41L, replacement.getVolatileValue());
            replacement.setVolatileValue(42L);
            assertEquals(42L, queue.tableStoreGet("test.closed.value"));
        }
    }

    // A retention snapshot contains committed consumer positions but no lock/version records.
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

    // Case-distinct persisted ids remain distinct because each may pin a different roll.
    @Test
    public void namedTailerIndexesKeepCaseVariantIdsSeparate() {
        NavigableMap<String, Long> metadataIndexes = new TreeMap<>();
        metadataIndexes.put("Gateway", 1L);
        metadataIndexes.put("gateway", 2L);
        metadataIndexes.put("gateway.lock", 3L);

        expectException("Named tailer id 'gateway.lock'");
        NavigableMap<String, Long> indexes = SingleChronicleQueue.selectNamedTailerIndexes(metadataIndexes);

        assertEquals(3, indexes.size());
        assertEquals(Long.valueOf(1L), indexes.get("Gateway"));
        assertEquals(Long.valueOf(2L), indexes.get("gateway"));
        assertEquals(Long.valueOf(3L), indexes.get("gateway.lock"));
    }

    // Legacy metadata-shaped ids are retained when nested records make them distinguishable.
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

            expectException("Named tailer id 'gateway.LOCK'");
            expectException("Named tailer id '" + replicated + ".lock'");
            expectException("Named tailer id '" + replicated + ".version'");
            NavigableMap<String, Long> indexes = q.namedTailerIndexes();

            assertEquals(Long.valueOf(index), indexes.get("gateway.LOCK"));
            assertEquals(Long.valueOf(index), indexes.get(replicated + ".lock"));
            assertEquals(Long.valueOf(index), indexes.get(replicated + ".version"));
            assertFalse(indexes.containsKey(replicated + ".LOCK.lock"));
            assertFalse(indexes.containsKey(replicated + ".LOCK.version"));
            assertFalse(indexes.containsKey(replicated + ".Version.lock"));
            assertFalse(indexes.containsKey(replicated + ".Version.version"));
        }
    }

    // A snapshot is point-in-time state and must not change with later tailer movement.
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

    // Parking an ordinary consumer resets only its persisted position to the never-read sentinel.
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

    // Parking an unknown id reports NOT_FOUND without creating a metadata entry.
    @Test
    public void parkNamedTailerDoesNotCreateMissingTailer() {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(30_000));
        writeDailyExcerpts(dir, 1);

        try (SingleChronicleQueue q = builder(dir).build()) {
            assertEquals(NamedTailerParkResult.NOT_FOUND, q.parkNamedTailer("missing"));
            assertThrows(NullPointerException.class, () -> q.parkNamedTailer(null));
            assertFalse(q.namedTailerIndexes().containsKey("missing"));
        }
    }

    // Metadata-shaped ids are rejected before their owner's lock or version can be altered.
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

                IllegalArgumentException versionException = assertThrows(IllegalArgumentException.class,
                        () -> q.parkNamedTailer("gateway.version"));
                assertTrue(versionException.getMessage().contains("would collide with the metadata"));
                assertEquals(42L, version.getValue());

                IllegalArgumentException lockException = assertThrows(IllegalArgumentException.class,
                        () -> q.parkNamedTailer("gateway.lock"));
                assertTrue(lockException.getMessage().contains("would collide with the metadata"));
                assertEquals(lockBefore, q.tableStoreGet("index.gateway.lock"));
            } finally {
                lock.unlock();
            }
        }
    }

    // Replicated consumer state is version coordinated and cannot be reset locally.
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
                long lockBefore = q.tableStoreGet("index." + name + ".lock");
                NavigableMap<String, Long> indexesBefore = q.namedTailerIndexes();

                assertEquals(NamedTailerParkResult.REFUSED_REPLICATED, q.parkNamedTailer(name));
                assertThrows(IllegalArgumentException.class,
                        () -> q.parkNamedTailer("Replicated:sink"));
                assertEquals(Long.valueOf(pinned), q.namedTailerIndexes().get(name));
                assertEquals(versionBefore, version.getValue());
                assertEquals(lockBefore, q.tableStoreGet("index." + name + ".lock"));
                assertEquals(indexesBefore, q.namedTailerIndexes());
            }
        }
    }

    // Case-insensitive table lookup must not let a prefix variant bypass replication safety.
    @Test
    public void maintenanceParkingRejectsMixedCaseReplicatedPrefix() {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(55_000));
        writeDailyExcerpts(dir, 1);

        try (SingleChronicleQueue q = builder(dir).build()) {
            String canonical = SingleChronicleQueue.REPLICATED_NAMED_TAILER_PREFIX + "sink";
            try (ExcerptTailer tailer = q.createTailer(canonical)) {
                assertEquals(0, tailer.index());
            }
            NavigableMap<String, Long> before = q.namedTailerIndexes();

            assertThrows(IllegalArgumentException.class,
                    () -> q.parkNamedTailer("Replicated:sink"));

            assertEquals(before, q.namedTailerIndexes());
        }
    }

    // Reserved suffix checks follow the table's case-insensitive key semantics.
    @Test
    public void parkNamedTailerRejectsExistingMixedCaseSuffixWithoutMutation() {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(61_000));
        writeDailyExcerpts(dir, 1);

        try (SingleChronicleQueue q = builder(dir).build()) {
            final long index = q.rollCycle().toIndex(q.firstCycle(), 0);
            q.tableStorePut("index.gateway.LOCK", index);

            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                    () -> q.parkNamedTailer("gateway.LOCK"));
            assertTrue(exception.getMessage().contains("would collide with the metadata"));
            expectException("Named tailer id 'gateway.LOCK'");
            assertEquals(Long.valueOf(index), q.namedTailerIndexes().get("gateway.LOCK"));
        }
    }

    // Registration participates in the metadata exclusive-lock protocol.
    @Test
    public void namedTailerRegistrationWaitsForExclusiveMetadataLock() throws Exception {
        assertNamedTailerRegistrationWaitsForMetadataLock(false);
    }

    // Registration also waits behind a maintenance reader's shared metadata lock.
    @Test
    public void namedTailerRegistrationWaitsForSharedMetadataLock() throws Exception {
        assertNamedTailerRegistrationWaitsForMetadataLock(true);
    }

    // Repeated snapshots remain usable while another Queue registers tailers under the same lock protocol.
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

    // The parked sentinel remains visible after a clean Queue reconstruction.
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
