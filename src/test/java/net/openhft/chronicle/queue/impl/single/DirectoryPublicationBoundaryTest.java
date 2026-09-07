/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class DirectoryPublicationBoundaryTest extends QueueTestCommon {
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

}
