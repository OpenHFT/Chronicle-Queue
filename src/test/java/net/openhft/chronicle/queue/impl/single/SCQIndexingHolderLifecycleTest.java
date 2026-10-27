/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.CleaningThread;
import net.openhft.chronicle.core.threads.CleaningThreadLocal;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueSystemProperties;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.WireType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class SCQIndexingHolderLifecycleTest {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void indexedAppendsKeepOnePairAcrossGc() throws Exception {
        gcLookup(false, false);
    }

    @Test
    public void checkedAppendsKeepOnePairAcrossGc() throws Exception {
        gcLookup(true, false);
    }

    @Test
    public void indexedSeeksKeepOnePairAcrossGc() throws Exception {
        gcLookup(false, true);
    }

    @Test
    public void checkedSeeksKeepOnePairAcrossGc() throws Exception {
        gcLookup(true, true);
    }

    private void gcLookup(boolean checkIndex, boolean seek) throws Exception {
        boolean saved = QueueSystemProperties.CHECK_INDEX;
        QueueSystemProperties.CHECK_INDEX = checkIndex;
        BytesStore<?, ?> store = null;
        try {
            try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(temporary.newFolder())
                    .rollCycle(TestRollCycles.TEST4_DAILY).blockSize(4096).build();
                 ExcerptAppender appender = queue.createAppender();
                 ExcerptTailer tailer = queue.createTailer()) {
                for (int i = 0; i < 9; i++)
                    appender.writeText("m" + i);
                long index = appender.lastIndexAppended() - 4;
                assertTrue(tailer.moveToIndex(index));
                assertEquals("m4", tailer.readText());
                SCQIndexing indexing = Jvm.getValue(Jvm.getValue(appender, "store"), "indexing");
                int reservations = indexing.closeables.size();
                assertTrue("Both index views must have been created", reservations >= 2);
                store = appender.wire().bytes().bytesStore();
                int refs = store.refCount();
                for (int cycle = 0; cycle < 4; cycle++) {
                    observeGc(indexing);
                    if (seek) {
                        assertTrue(tailer.moveToIndex(index));
                        assertEquals("m4", tailer.readText());
                    } else {
                        for (int i = 0; i < 4; i++)
                            appender.writeText("cycle" + cycle + "-" + i);
                    }
                    assertEquals("GC must not accumulate owned index views", reservations, indexing.closeables.size());
                    assertEquals("GC must not add mapped-store reservations", refs, store.refCount());
                }
            }
        } finally {
            QueueSystemProperties.CHECK_INDEX = saved;
            BackgroundResourceReleaser.releasePendingResources();
        }
        assertNotNull(store);
        assertEquals("Every mapped-store owner must release on close", 0, store.refCount());
    }

    private static void observeGc(SCQIndexing indexing) throws InterruptedException {
        WeakReference<Object> control = unreachableControl();
        for (int i = 0; control.get() != null && i < 100; i++) {
            System.gc();
            Thread.sleep(10);
        }
        assertNull("GC control was not collected; the precondition was not established", control.get());
        // When this regression is run against the old source, prove its weak holders cleared.
        for (String name : new String[]{"indexArray", "index2indexArray"}) {
            Object cached = Jvm.<ThreadLocal<?>>getValue(indexing, name).get();
            if (cached instanceof WeakReference)
                assertNull("Baseline holder must clear before the lookup", ((WeakReference<?>) cached).get());
        }
    }

    private static WeakReference<Object> unreachableControl() {
        return new WeakReference<>(new Object());
    }

    @Test
    public void cleaningThreadReleasesItsViewsOnExit() throws Exception {
        threadExit(true);
    }

    @Test
    public void ordinaryThreadCleanupReleasesItsViews() throws Exception {
        threadExit(false);
    }

    private static void threadExit(boolean cleaning) throws Exception {
        try (SCQIndexing indexing = new SCQIndexing(WireType.BINARY, 8, 1)) {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Runnable action = () -> {
                try {
                    createViews(indexing);
                } catch (Throwable t) {
                    failure.set(t);
                }
            };
            Thread thread = cleaning ? new CleaningThread(action, "index-view-owner") : new Thread(action, "index-view-owner");
            thread.start();
            thread.join(5000);
            assertFalse("Owner must terminate", thread.isAlive());
            assertNull(failure.get());
            CleaningThreadLocal.cleanupNonCleaningThreads();
            assertTrue("Terminated owners must not retain index views", indexing.closeables.isEmpty());
        }
    }

    @Test
    public void closingManyOwnersFromAnotherThreadReleasesTheirViews() throws Exception {
        ExecutorService owner = Executors.newSingleThreadExecutor();
        try {
            for (int cycle = 0; cycle < 32; cycle++) {
                SCQIndexing indexing = new SCQIndexing(WireType.BINARY, 8, 1);
                List<Closeable> views = owner.submit(() -> {
                    createViews(indexing);
                    return new ArrayList<>(indexing.closeables);
                }).get(5, TimeUnit.SECONDS);
                assertEquals(2, views.size());
                indexing.close();
                indexing.close();
                assertTrue(indexing.closeables.isEmpty());
                for (Closeable view : views)
                    assertTrue("Cross-thread owner close must release the view", view.isClosed());
            }
        } finally {
            owner.shutdownNow();
            assertTrue(owner.awaitTermination(5, TimeUnit.SECONDS));
            CleaningThreadLocal.cleanupNonCleaningThreads();
        }
    }

    private static void createViews(SCQIndexing indexing) throws Exception {
        for (String name : new String[]{"getIndexArray", "getIndex2IndexArray"}) {
            Method method = SCQIndexing.class.getDeclaredMethod(name);
            method.setAccessible(true);
            method.invoke(indexing);
        }
        assertEquals(2, indexing.closeables.size());
    }
}
