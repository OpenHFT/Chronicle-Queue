/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.Wire;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static org.junit.Assert.*;

public class StoreAcquisitionDiagnosticTest extends QueueTestCommon {
    @Test
    public void internalErrorRemainsPrimaryAndReleasesBytes() throws Exception {
        factoryInternalError(false);
    }

    @Test
    public void internalErrorAfterBytesCloseRemainsPrimary() throws Exception {
        factoryInternalError(true);
    }

    private void factoryInternalError(boolean closesBytes) throws Exception {
        AtomicReference<Bytes<?>> captured = new AtomicReference<>();
        InternalError expected = new InternalError("injected native store failure");
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
                .testBlockSize().rollCycle(TEST_DAILY).timeProvider(new SetTimeProvider(0)).build()) {
            Field factory = SingleChronicleQueue.class.getDeclaredField("storeFactory");
            factory.setAccessible(true);
            BiFunction<RollingChronicleQueue, Wire, SingleChronicleQueueStore> throwing = (q, wire) -> {
                captured.set(wire.bytes());
                if (closesBytes)
                    wire.bytes().releaseLast();
                throw expected;
            };
            factory.set(queue, throwing);
            try (ExcerptAppender appender = queue.createAppender()) {
                assertSame(expected, assertThrows(Throwable.class, () -> appender.writeText("value")));
                assertNotNull(captured.get());
                assertEquals("Acquisition releases bytes before Queue.close", 0, captured.get().refCount());
            } finally {
                // Keep the failing baseline test from leaking its deliberately captured reservation.
                if (captured.get() != null && captured.get().refCount() > 0)
                    captured.get().releaseLast();
            }
        }
    }
}
