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

            TestMessageListener writer = queue.acquireAppender().methodWriterBuilder(TestMessageListener.class).build();
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

    public interface TestMessageListener {
        void onMessage(ProxyTest.Message message);
    }

    public static class Message extends SelfDescribingMarshallable {

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
