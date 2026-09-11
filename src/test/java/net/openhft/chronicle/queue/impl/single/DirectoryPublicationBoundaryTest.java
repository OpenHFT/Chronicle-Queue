/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class DirectoryPublicationBoundaryTest extends QueueTestCommon {
    @Test
    public void fileSystemBoundsUseLogicalCycles() throws Exception {
        File path = getTmpDir();
        assertTrue(path.mkdirs());
        assertTrue(new File(path, "19700101.cq4").createNewFile());
        assertTrue(new File(path, "+58815800711.cq4").createNewFile());
        try (FileSystemDirectoryListing listing = new FileSystemDirectoryListing(path,
                name -> name.startsWith("+") ? Integer.MAX_VALUE : 0, () -> 0)) {
            listing.refresh(true);
            assertEquals(0, listing.getMinCreatedCycle());
            assertEquals(Integer.MAX_VALUE, listing.getMaxCreatedCycle());
        }
    }

    @Test
    public void readOnlyTailerSeesMinimumBeforeRefreshedMaximum() throws Exception {
        File path = getTmpDir();
        long time = 7L * TestRollCycles.TEST_DAILY.lengthInMillis();
        try (SingleChronicleQueue queue = builder(path, time).build()) {
            try (ExcerptAppender appender = queue.createAppender()) {
                appender.writeText("cycle seven");
            }
            queue.tableStorePut("listing.lowestCycle", Integer.MAX_VALUE);
            queue.tableStorePut("listing.highestCycle", Integer.MIN_VALUE);
            Field listingField = SingleChronicleQueue.class.getDeclaredField("directoryListing");
            listingField.setAccessible(true);
            TableDirectoryListing listing = (TableDirectoryListing) listingField.get(queue);
            Field maximumField = TableDirectoryListing.class.getDeclaredField("maxCycleValue");
            maximumField.setAccessible(true);
            LongValue maximum = (LongValue) maximumField.get(listing);
            AtomicBoolean observed = new AtomicBoolean();
            LongValue intercepted = (LongValue) Proxy.newProxyInstance(LongValue.class.getClassLoader(),
                    new Class<?>[]{LongValue.class}, (proxy, method, args) -> {
                        try {
                            Object result = method.invoke(maximum, args);
                            if (method.getName().equals("compareAndSwapValue") && Boolean.TRUE.equals(result)
                                    && observed.compareAndSet(false, true)) {
                                // Pause publication after the actual maximum CAS, before refresh can continue.
                                try (SingleChronicleQueue reader = builder(path, time).readOnly(true).build();
                                     ExcerptTailer tailer = reader.createTailer()) {
                                    assertEquals(7, reader.firstCycle());
                                    assertEquals("cycle seven", tailer.readText());
                                }
                            }
                            return result;
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
            maximumField.set(listing, intercepted);
            try {
                queue.refreshDirectoryListing();
            } finally {
                maximumField.set(listing, maximum);
            }
            assertTrue(observed.get());
        }
    }

    @Test
    public void readOnlyRetryClosesEveryReturnedBinding() {
        List<AtomicBoolean> closed = new ArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        TableStore<?> table = (TableStore<?>) Proxy.newProxyInstance(TableStore.class.getClassLoader(),
                new Class<?>[]{TableStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("acquireValueFor")) {
                        if (calls.incrementAndGet() == 2)
                            throw new IllegalStateException("later binding not ready");
                        AtomicBoolean state = new AtomicBoolean();
                        closed.add(state);
                        return Proxy.newProxyInstance(LongValue.class.getClassLoader(), new Class<?>[]{LongValue.class},
                                (binding, operation, values) -> {
                                    if (operation.getName().equals("close")) {
                                        state.set(true);
                                        return null;
                                    }
                                    if (operation.getName().equals("isClosed"))
                                        return state.get();
                                    return defaultValue(operation.getReturnType());
                                });
                    }
                    return defaultValue(method.getReturnType());
                });
        try (TableDirectoryListingReadOnly listing = new TableDirectoryListingReadOnly(table, () -> 0)) {
            listing.init();
            assertEquals(4, closed.size());
            assertTrue("the failed attempt must release its returned binding", closed.get(0).get());
        }
        assertTrue(closed.stream().allMatch(AtomicBoolean::get));
    }

    private static Object defaultValue(Class<?> type) {
        if (type == boolean.class)
            return false;
        if (type == long.class)
            return 0L;
        if (type == int.class)
            return 0;
        return null;
    }

    private static SingleChronicleQueueBuilder builder(File path, long time) {
        return SingleChronicleQueueBuilder.binary(path).testBlockSize()
                .rollCycle(TestRollCycles.TEST_DAILY).timeProvider(() -> time);
    }
}
