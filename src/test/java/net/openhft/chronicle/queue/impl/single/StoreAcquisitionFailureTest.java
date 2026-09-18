/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.WireStoreFactory;
import net.openhft.chronicle.wire.ValueOut;
import net.openhft.chronicle.wire.Wire;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collection;

import static net.openhft.chronicle.queue.impl.WireStoreSupplier.CreateStrategy.CREATE;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class StoreAcquisitionFailureTest extends QueueTestCommon {
    @Parameterized.Parameters(name = "error={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[]{false}, new Object[]{true});
    }

    @Parameterized.Parameter
    public boolean error;

    @Test
    public void factoryFailureReleasesProvisionalBytes() {
        assertReleasedAfterFailure("factory");
    }

    @Test
    public void headerSerialisationFailureReleasesConstructedStore() {
        assertReleasedAfterFailure("header");
    }

    @Test
    public void indexInitialisationFailureReleasesConstructedStore() {
        assertReleasedAfterFailure("index");
    }

    private void assertReleasedAfterFailure(String phase) {
        Throwable failure = error ? new AssertionError("injected " + phase) : new IORuntimeException("injected " + phase);
        FailingBuilder builder = new FailingBuilder(phase, failure);
        builder.path(getTmpDir()).rollCycle(TEST_DAILY).testBlockSize();
        try {
            try (SingleChronicleQueue queue = builder.build()) {
                assertSame(failure, assertThrows(Throwable.class, () -> queue.storeSupplier().acquire(queue.cycle(), CREATE)));
            }
            BackgroundResourceReleaser.releasePendingResources();
            assertNotNull(builder.bytes);
            if (builder.store != null)
                assertTrue("Constructed store must be closed", builder.store.isClosed());
            assertEquals("Provisional bytes reservation", 0, builder.bytes.refCount());
            assertEquals("Mapped file reservations after Queue.close", 0, builder.bytes.mappedFile().refCount());
        } finally {
            // Keep a deliberately failing baseline run from leaking into the following parameter/test.
            Closeable.closeQuietly(builder.store, builder.bytes);
            BackgroundResourceReleaser.releasePendingResources();
        }
    }

    private static final class FailingBuilder extends SingleChronicleQueueBuilder {
        private final String phase;
        private final Throwable failure;
        private MappedBytes bytes;
        private SingleChronicleQueueStore store;

        private FailingBuilder(String phase, Throwable failure) {
            this.phase = phase;
            this.failure = failure;
        }

        @Override
        public WireStoreFactory storeFactory() {
            return (queue, wire) -> {
                bytes = (MappedBytes) wire.bytes();
                if ("factory".equals(phase))
                    throw Jvm.rethrow(failure);
                if ("header".equals(phase)) {
                    Wire failingWire = (Wire) Proxy.newProxyInstance(Wire.class.getClassLoader(), new Class<?>[]{Wire.class},
                            (proxy, method, args) -> {
                                if ("writeEventName".equals(method.getName()))
                                    return Proxy.newProxyInstance(ValueOut.class.getClassLoader(), new Class<?>[]{ValueOut.class},
                                            (valueProxy, valueMethod, valueArgs) -> {
                                                if ("typedMarshallable".equals(valueMethod.getName())) {
                                                    store = (SingleChronicleQueueStore) valueArgs[0];
                                                    throw failure;
                                                }
                                                throw new AssertionError("Unexpected call: " + valueMethod);
                                            });
                                try {
                                    return method.invoke(wire, args);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                            });
                    return createStore(queue, failingWire);
                }
                store = new SingleChronicleQueueStore(queue.rollCycle(), queue.wireType(), bytes,
                        queue.indexCount(), queue.indexSpacing()) {
                    @Override
                    public void initIndex(Wire wire) {
                        throw Jvm.rethrow(failure);
                    }
                };
                return store;
            };
        }
    }
}
