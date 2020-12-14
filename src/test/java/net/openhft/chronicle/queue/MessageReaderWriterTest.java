/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
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

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RequiredForClient
public class MessageReaderWriterTest extends ChronicleQueueTestBase {

    @Test
    public void testWriteWhileReading() {
        ClassAliasPool.CLASS_ALIASES.addAlias(Message1.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(Message2.class);

        File path1 = getTmpDir();
        File path2 = getTmpDir();

        try (ChronicleQueue queue1 = SingleChronicleQueueBuilder
                .binary(path1)
                .testBlockSize()
                .build();
             ChronicleQueue queue2 = SingleChronicleQueueBuilder
                     .binary(path2)
                     .testBlockSize()
                     .build()) {
            MethodReader reader2 = queue1.createTailer().methodReader(ObjectUtils.printAll(MessageListener.class));
            MessageListener writer2 = queue2.acquireAppender().methodWriter(MessageListener.class);
            MessageListener processor = new MessageProcessor(writer2);
            MethodReader reader1 = queue1.createTailer().methodReader(processor);
            MessageListener writer1 = queue1.acquireAppender().methodWriter(MessageListener.class);

            for (int i = 0; i < 3; i++) {
                // write a message
                writer1.method1(new Message1("hello"));
                writer1.method2(new Message2(234));

                // read those messages
                assertTrue(reader1.readOne());
                assertTrue(reader1.readOne());
                assertFalse(reader1.readOne());

                // read the produced messages
                assertTrue(reader2.readOne());
                assertTrue(reader2.readOne());
                assertFalse(reader2.readOne());
            }
           // System.out.println(queue1.dump());
        }
    }

    interface MessageListener {
        void method1(Message1 message);

        void method2(Message2 message);
    }

    static class Message1 extends SelfDescribingMarshallable {
        String text;

        public Message1(String text) {
            this.text = text;
        }
    }

    static class Message2 extends SelfDescribingMarshallable {
        long number;

        public Message2(long number) {
            this.number = number;
        }
    }

    static class MessageProcessor implements MessageListener {
        private final MessageListener writer2;

        public MessageProcessor(MessageListener writer2) {
            this.writer2 = writer2;
        }

        @Override
        public void method1(@NotNull Message1 message) {
            message.text += "-processed";
            writer2.method1(message);
        }

        @Override
        public void method2(@NotNull Message2 message) {
            message.number += 1000;
            writer2.method2(message);
        }
    }
}
