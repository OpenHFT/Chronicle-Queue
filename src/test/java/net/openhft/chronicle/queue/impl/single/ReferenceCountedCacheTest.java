/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractReferenceCounted;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.ReferenceCounted;
import net.openhft.chronicle.core.io.ReferenceOwner;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Before;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

class ReferenceCountedCacheTest extends QueueTestCommon {

    private static final int MAX_THREADS_TO_RUN = 6;
    private static final int MIN_THREADS_TO_RUN = 3;
    private static final int NUM_RESOURCES = 50;
    private AtomicInteger createdCount;
    private AtomicInteger releasedCount;
    private ReferenceCountedCache<Integer, TestReferenceCounted, Reservation, RuntimeException> cache;

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @BeforeEach
    void setUp() {
        createdCount = new AtomicInteger(0);
        releasedCount = new AtomicInteger(0);
        cache = new ReferenceCountedCache<>(Reservation::new, id -> new TestReferenceCounted());
    }

    @AfterEach
    void closeCache() {
        Closeable.closeQuietly(cache);
    }

    @Test
    void shouldNeverGiveOutReleasedReferences() {
        final ExecutorService executorService = Executors.newCachedThreadPool();
        int numThreads = Math.min(MAX_THREADS_TO_RUN, Math.max(MIN_THREADS_TO_RUN, Runtime.getRuntime().availableProcessors()));
        AtomicBoolean running = new AtomicBoolean(true);
        List<Future<?>> executors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            executors.add(executorService.submit(new ReferenceGetter(NUM_RESOURCES, cache, running)));
        }
        Jvm.pause(5_000);
        running.set(false);
        executors.forEach(f -> {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        Closeable.closeQuietly(cache);
        Jvm.startup().on(ReferenceCountedCache.class, "Created " + createdCount.get() + ", released " + releasedCount.get());
        assertTrue(true); // if we got here without an exception, the test passes
    }

    @Test
    void shouldReturnSameObjectWhileNotReleased() {
        final Reservation reservation = cache.get(1);
        final Reservation otherReservation = cache.get(1);
        Assertions.assertSame(reservation.referenceCounted, otherReservation.referenceCounted);
        reservation.release();
        otherReservation.release();
    }

    @Test
    void shouldExpireObjectsWhenReferenceCountGoesToZero() {
        final Reservation reservation = cache.get(1);
        reservation.release();
        Jvm.pause(100);
        final Reservation otherReservation = cache.get(1);
        Assertions.assertNotSame(reservation.referenceCounted, otherReservation.referenceCounted);
        otherReservation.release();
    }

    private static class ReferenceGetter implements Runnable, ReferenceOwner {

        private final int numResources;
        private final ReferenceCountedCache<Integer, TestReferenceCounted, Reservation, RuntimeException> cache;
        private final AtomicBoolean running;
        private final Random random;
        private final Reservation[] reservations;

        ReferenceGetter(int numResources, ReferenceCountedCache<Integer, TestReferenceCounted, Reservation, RuntimeException> referenceId, AtomicBoolean running) {
            this.numResources = numResources;
            this.cache = referenceId;
            this.running = running;
            this.random = ThreadLocalRandom.current();
            this.reservations = new Reservation[numResources];
        }

        @Override
        public void run() {
            int reservationCount = 0;
            while (running.get()) {
                int nextResource = random.nextInt(numResources);
                if (reservations[nextResource] != null) {
                    reservations[nextResource].release();
                    reservations[nextResource] = null;
                } else {
                    reservations[nextResource] = cache.get(nextResource);
                    reservations[nextResource].assertNotReleased();
                    reservationCount++;
                }
            }
            for (int i = 0; i < reservations.length; i++) {
                if (reservations[i] != null) {
                    reservations[i].release();
                }
            }
            Jvm.startup().on(ReferenceGetter.class, "Made " + reservationCount + " reservations");
        }
    }

    private static class Reservation {

        private final ReferenceCounted referenceCounted;
        private final ReferenceOwner referenceOwner;

        Reservation(ReferenceCounted referenceCounted) {
            this.referenceOwner = ReferenceOwner.temporary("reservation");
            this.referenceCounted = referenceCounted;
            referenceCounted.reserve(referenceOwner);
        }

        void release() {
            this.referenceCounted.release(referenceOwner);
        }

        void assertNotReleased() {
            final int referenceCount = this.referenceCounted.refCount();
            assertTrue("Expected reference count of at least 2, got " + referenceCount, referenceCount > 1);
        }
    }

    private class TestReferenceCounted extends AbstractReferenceCounted implements ReferenceOwner, Closeable {

        TestReferenceCounted() {
            createdCount.incrementAndGet();
        }

        @Override
        protected void performRelease() throws IllegalStateException {
            releasedCount.incrementAndGet();
        }

        @Override
        public void close() {
        }

        @Override
        public boolean isClosed() {
            return false;
        }
    }
}
