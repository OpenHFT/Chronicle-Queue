/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@RequiredForClient
public class MethodReaderObjectReuseTest extends QueueTestCommon {
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
        } finally {
            IOTools.deleteDirWithFiles(path);
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
