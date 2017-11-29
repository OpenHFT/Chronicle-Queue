package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/*
 * Created by Peter Lawrey on 08/05/2017.
 */
public class MethodReaderObjectReuseTest {
    @Test
    public void testOneOne() {
        ClassAliasPool.CLASS_ALIASES.addAlias(PingDTO.class);
        try (ChronicleQueue cq = ChronicleQueueBuilder.single(OS.TARGET + "/MethodReaderObjectReuseTest-" + System.nanoTime()).build()) {
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
            assertEquals(PingDTO.constructionExpected, PingDTO.constructionCounter);
            while (reader.readOne()) ;
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

    static class PingDTO extends AbstractMarshallable {
        static int constructionCounter, constructionExpected;
        final Bytes bytes = Bytes.allocateElasticDirect();

        PingDTO() {
            if (++constructionCounter > constructionExpected)
                throw new AssertionError();
        }
    }
}
