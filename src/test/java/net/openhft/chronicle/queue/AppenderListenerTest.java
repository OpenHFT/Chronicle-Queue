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

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AppenderListenerTest extends QueueTestCommon {

    @Test
    public void appenderListenerTest() {
        String path = OS.getTarget() + "/appenderListenerTest";
        StringBuilder results = new StringBuilder();
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(path)
                .testBlockSize()
                .appenderListener((wire, index) -> {
                    long offset = ((index >>> 32) << 40) | wire.bytes().readPosition();
                    String event = wire.readEvent(String.class);
                    String text = wire.getValueIn().text();
                    results.append(event)
                            .append(" ").append(text)
                            .append(", addr:").append(Long.toHexString(offset))
                            .append(", index: ").append(Long.toHexString(index)).append("\n");
                })
                .timeProvider(new SetTimeProvider("2021/11/29T13:53:59").advanceMillis(1000))
                .build();
             ExcerptAppender appender = q.acquireAppender()) {
            final HelloWorld writer = appender.methodWriter(HelloWorld.class);
            writer.hello("G'Day");
            writer.hello("Bye-now");
        }
        IOTools.deleteDirWithFiles(path);
        assertEquals("" +
                "hello G'Day, addr:4a100000010114, index: 4a1000000000\n" +
                "hello Bye-now, addr:4a100000010128, index: 4a1000000001\n", results.toString());
    }

    public interface HelloWorld {
        void hello(String s);
    }
}