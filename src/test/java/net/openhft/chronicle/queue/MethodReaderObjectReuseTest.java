package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@RequiredForClient
public class MethodReaderObjectReuseTest extends QueueTestCommon {
    @Test
    public void testOneOne() {
        ClassAliasPool.CLASS_ALIASES.addAlias(PingDTO.class);
        try (ChronicleQueue cq = SingleChronicleQueueBuilder.single(OS.getTarget() + "/MethodReaderObjectReuseTest-" + Time.uniqueId()).build()) {
            PingDTO.constructionExpected++;
            PingDTO pdtio = new PingDTO();
            PingDTO.constructionExpected++;
            Pinger pinger = cq.acquireAppender().methodWriter(Pinger.class);
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
            while (reader.readOne()) ;
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
        }
    }

    @FunctionalInterface
    interface Pinger {
        void ping(PingDTO pingDTO);
    }

    static class PingDTO extends SelfDescribingMarshallable {
        static int constructionCounter, constructionExpected;
        final Bytes<?> bytes = Bytes.allocateElasticOnHeap();

        PingDTO() {
            if (++constructionCounter > constructionExpected)
                throw new AssertionError();
        }
    }
}
