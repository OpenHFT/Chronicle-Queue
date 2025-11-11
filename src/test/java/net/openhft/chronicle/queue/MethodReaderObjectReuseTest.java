/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RequiredForClient
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MethodReaderObjectReuseTest extends QueueTestCommon {
    @Before
    public void resetCounters() {
        PingDTO.constructionCounter = 0;
        PingDTO.constructionExpected = 0;
    }

    @Test
    public void testOneOne() {
        ClassAliasPool.CLASS_ALIASES.addAlias(PingDTO.class);
        String path = OS.getTarget() + "/MethodReaderObjectReuseTest-" + Time.uniqueId();
        try (ChronicleQueue cq = SingleChronicleQueueBuilder.single(path).build()) {
            PingDTO.constructionExpected++;
            PingDTO pdtio = new PingDTO();
            PingDTO.constructionExpected++;
            Pinger pinger = cq.methodWriter(Pinger.class);
            for (int i = 0; i < 5; i++) {
                pinger.ping(pdtio);
                assertEquals(PingDTO.constructionExpected, PingDTO.constructionCounter);
                pdtio.bytes.append("hi");
            }
            StringBuilder sb = new StringBuilder();
            PingDTO.constructionExpected++;
            MethodReader reader = cq.createTailer()
                    .methodReader(
                            (Pinger) pingDTO -> sb.append("ping ").append(pingDTO));
            while (reader.readOne()) {
                // exhaust reader
            }
            // moved this assert below the readOne as object may be constructed lazily
            assertEquals(PingDTO.constructionExpected, PingDTO.constructionCounter);
            assertEquals("ping !PingDTO {\n" +
                    "  bytes: \"\"\n" +
                    "}\n" +
                    "ping !PingDTO {\n" +
                    "  bytes: hi\n" +
                    "}\n" +
                    "ping !PingDTO {\n" +
                    "  bytes: hihi\n" +
                    "}\n" +
                    "ping !PingDTO {\n" +
                    "  bytes: hihihi\n" +
                    "}\n" +
                    "ping !PingDTO {\n" +
                    "  bytes: hihihihi\n" +
                    "}\n", sb.toString());
        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }

    @Test
    public void testPayloadSnapshotWhenSourceMutates() {
        ClassAliasPool.CLASS_ALIASES.addAlias(PingDTO.class);
        String path = OS.getTarget() + "/MethodReaderObjectReuseTest-snapshot-" + Time.uniqueId();
        try (ChronicleQueue cq = SingleChronicleQueueBuilder.single(path).build()) {
            PingDTO.constructionCounter = 0;
            PingDTO.constructionExpected = 10;
            Pinger pinger = cq.methodWriter(Pinger.class);
            PingDTO.constructionExpected++;
            PingDTO dto = new PingDTO();
            dto.bytes.append("immutable");
            pinger.ping(dto);
            dto.bytes.clear().append("mutated-after-write");

            AtomicReference<String> observed = new AtomicReference<>();
            PingDTO.constructionExpected++;
            MethodReader reader = cq.createTailer().methodReader(
                    (Pinger) pingDTO -> observed.set(pingDTO.bytes.toString()));
            assertTrue(reader.readOne());
            assertEquals("immutable", observed.get());
        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }

    @Test
    public void testZZDirectBytesSnapshotWhenSourceMutates() {
        ClassAliasPool.CLASS_ALIASES.addAlias(DirectPingDTO.class);
        String path = OS.getTarget() + "/MethodReaderObjectReuseTest-direct-" + Time.uniqueId();
        try (ChronicleQueue cq = SingleChronicleQueueBuilder.single(path).build()) {
            DirectPinger pinger = cq.methodWriter(DirectPinger.class);
            DirectPingDTO dto = new DirectPingDTO();
            dto.bytes.append("direct");
            pinger.ping(dto);
            dto.bytes.clear().append("post-write-mutation");

            AtomicReference<String> observed = new AtomicReference<>();
            MethodReader reader = cq.createTailer().methodReader(
                    (DirectPinger) pingDTO -> observed.set(pingDTO.bytes.toString()));
            assertTrue(reader.readOne());
            assertEquals("direct", observed.get());
            dto.close();
        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }

    @FunctionalInterface
    interface Pinger {
        void ping(PingDTO pingDTO);
    }

    @FunctionalInterface
    interface DirectPinger {
        void ping(DirectPingDTO pingDTO);
    }

    static class PingDTO extends SelfDescribingMarshallable {
        static int constructionCounter, constructionExpected;
        final Bytes<?> bytes = Bytes.allocateElasticOnHeap();

        PingDTO() {
            if (++constructionCounter > constructionExpected)
                throw new AssertionError();
        }
    }

    static final class DirectPingDTO extends SelfDescribingMarshallable implements AutoCloseable {
        final Bytes<?> bytes = Bytes.allocateElasticDirect();

        @Override
        public void close() {
            bytes.releaseLast();
        }
    }
}
