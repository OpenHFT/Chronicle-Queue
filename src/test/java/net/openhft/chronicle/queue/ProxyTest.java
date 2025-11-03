/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * To avoid the arg[] array created via the methodWriter java.lang.reflect.Proxy, this test shows how you can create a custom proxy
 * <p>
 * Created by Rob Austin
 */
public class ProxyTest extends QueueTestCommon {

    @Test
    public void testReadWrite() {

        File tempDir = getTmpDir();
        StringBuilder result = new StringBuilder();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tempDir).build()) {

            TestMessageListener writer = queue.methodWriterBuilder(TestMessageListener.class).build();
            Message message = new ProxyTest.Message();

            StringBuilder sb = new StringBuilder("test ");
            int length = sb.length();

            for (int i = 0; i < 10; i++) {
                sb.append(i);
                message.message(sb);
                writer.onMessage(message);
                sb.setLength(length);
            }

            MethodReader methodReader = queue.createTailer().methodReader(new TestMessageListener() {

                @Override
                public void onMessage(final Message message) {
                    result.append(message);
                }
            });

            for (int i = 0; i < 10; i++) {
                methodReader.readOne();
            }
        }
        Assert.assertEquals("!net.openhft.chronicle.queue.ProxyTest$Message {\n" +
                "  message: test 0\n" +
                "}\n" +
                "!net.openhft.chronicle.queue.ProxyTest$Message {\n" +
                "  message: test 1\n" +
                "}\n" +
                "!net.openhft.chronicle.queue.ProxyTest$Message {\n" +
                "  message: test 2\n" +
                "}\n" +
                "!net.openhft.chronicle.queue.ProxyTest$Message {\n" +
                "  message: test 3\n" +
                "}\n" +
                "!net.openhft.chronicle.queue.ProxyTest$Message {\n" +
                "  message: test 4\n" +
                "}\n" +
                "!net.openhft.chronicle.queue.ProxyTest$Message {\n" +
                "  message: test 5\n" +
                "}\n" +
                "!net.openhft.chronicle.queue.ProxyTest$Message {\n" +
                "  message: test 6\n" +
                "}\n" +
                "!net.openhft.chronicle.queue.ProxyTest$Message {\n" +
                "  message: test 7\n" +
                "}\n" +
                "!net.openhft.chronicle.queue.ProxyTest$Message {\n" +
                "  message: test 8\n" +
                "}\n" +
                "!net.openhft.chronicle.queue.ProxyTest$Message {\n" +
                "  message: test 9\n" +
                "}\n", result.toString());
    }

    interface TestMessageListener {
        void onMessage(ProxyTest.Message message);
    }

    static class Message extends SelfDescribingMarshallable {

        private final StringBuilder message = new StringBuilder();

        CharSequence message() {
            return message;
        }

        Message message(final CharSequence message) {
            this.message.setLength(0);
            this.message.append(message);
            return this;
        }
    }
}
