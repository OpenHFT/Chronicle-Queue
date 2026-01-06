/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@RequiredForClient
public class MessageReaderWriterTest extends QueueTestCommon {

    @Test
    @DisplayName("Reader can write messages while reading")
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
            MethodReader reader2 = queue1.createTailer().methodReader(printAll(MessageListener.class));
            MessageListener writer2 = queue2.methodWriter(MessageListener.class);
            MessageListener processor = new MessageProcessor(writer2);
            MethodReader reader1 = queue1.createTailer().methodReader(processor);
            MessageListener writer1 = queue1.methodWriter(MessageListener.class);

            for (int i = 0; i < 3; i++) {
                // write a message
                writer1.method1(new Message1("hello"));
                writer1.method2(new Message2(234));

                // read those messages
                assertTrue(reader1.readOne(), "reader1 should read first message at iteration " + i);
                assertTrue(reader1.readOne(), "reader1 should read second message at iteration " + i);
                assertFalse(reader1.readOne(), "reader1 should have no further messages at iteration " + i);

                // read the produced messages
                assertTrue(reader2.readOne(), "reader2 should read first processed message at iteration " + i);
                assertTrue(reader2.readOne(), "reader2 should read second processed message at iteration " + i);
                assertFalse(reader2.readOne(), "reader2 should have no further messages at iteration " + i);
            }
        }
    }

    private static <T> T printAll(@NotNull Class<T> tClass, Class<?>... additional) throws IllegalArgumentException {
        return ObjectUtils.onMethodCall((method, args) -> {
            @NotNull String argsStr = args == null ? "()" : Arrays.toString(args);
            System.out.println(method.getName() + " " + argsStr);
            return ObjectUtils.defaultValue(method.getReturnType());
        }, tClass, additional);
    }

    interface MessageListener {
        void method1(Message1 message);

        void method2(Message2 message);
    }

    static class Message1 extends SelfDescribingMarshallable {
        String text;

        Message1(String text) {
            this.text = text;
        }
    }

    static class Message2 extends SelfDescribingMarshallable {
        long number;

        Message2(long number) {
            this.number = number;
        }
    }

    static class MessageProcessor implements MessageListener {
        private final MessageListener writer2;

        MessageProcessor(MessageListener writer2) {
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
