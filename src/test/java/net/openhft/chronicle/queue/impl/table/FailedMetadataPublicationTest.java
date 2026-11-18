/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.wire.ValueOut;
import net.openhft.chronicle.wire.Wire;
import org.junit.Test;

import java.io.File;
import java.io.StreamCorruptedException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class FailedMetadataPublicationTest extends QueueTestCommon {
    @Test
    public void corruptHeaderReleasesUnpublishedBinding() throws Exception {
        checkFailedPublication(new StreamCorruptedException("injected header conflict"));
    }

    @Test
    public void runtimeFailureReleasesUnpublishedBinding() throws Exception {
        checkFailedPublication(new IllegalStateException("injected publication failure"));
    }

    @Test
    public void errorReleasesUnpublishedBinding() throws Exception {
        checkFailedPublication(new AssertionError("injected publication error"));
    }

    private void checkFailedPublication(Throwable failure) throws Exception {
        File file = new File(getTmpDir(), "metadata.cq4t");
        Files.createDirectories(file.toPath().getParent());
        AtomicReference<LongValue> binding = new AtomicReference<>();
        try (TableStore<Metadata.NoMeta> table = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build()) {
            Wire original = (Wire) Jvm.getField(SingleTableStore.class, "mappedWire").get(table);
            Wire failing = (Wire) Proxy.newProxyInstance(Wire.class.getClassLoader(), new Class<?>[]{Wire.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("updateHeader"))
                            throw failure;
                        Object result = invoke(original, method, args);
                        if (method.getName().equals("writeEventName")) {
                            ValueOut out = (ValueOut) result;
                            return Proxy.newProxyInstance(ValueOut.class.getClassLoader(), new Class<?>[]{ValueOut.class},
                                    (valueProxy, valueMethod, valueArgs) -> {
                                        if (valueMethod.getName().equals("int64forBinding"))
                                            binding.set((LongValue) valueArgs[1]);
                                        return invoke(out, valueMethod, valueArgs);
                                    });
                        }
                        return result;
                    });
            Jvm.getField(SingleTableStore.class, "mappedWire").set(table, failing);
            try {
                Throwable caught = assertThrows(Throwable.class,
                        () -> table.doWithExclusiveLock(ts -> ts.acquireValueFor("failed-publication", 17)));
                if (failure instanceof StreamCorruptedException) {
                    assertTrue(caught instanceof IORuntimeException);
                    assertSame(failure, caught.getCause());
                    assertTrue(caught.getMessage().contains(file.toString()));
                    assertTrue(caught.getMessage().contains("readLimit="));
                    assertTrue(caught.getMessage().contains("writeLimit="));
                } else {
                    assertSame(failure, caught);
                }
                assertNotNull("the failure occurs after the value has been bound", binding.get());
                assertTrue("failed acquisition must release the unpublished binding", binding.get().isClosed());
            } finally {
                Jvm.getField(SingleTableStore.class, "mappedWire").set(table, original);
                if (binding.get() != null)
                    binding.get().close();
            }
        }
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
