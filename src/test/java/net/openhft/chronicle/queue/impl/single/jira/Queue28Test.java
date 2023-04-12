/*
 * Copyright 2016-2020 chronicle.software
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
package net.openhft.chronicle.queue.impl.single.jira;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class Queue28Test extends QueueTestCommon {

    private final WireType wireType;

    public Queue28Test(WireType wireType) {
        this.wireType = wireType;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
               // {WireType.TEXT},
                {WireType.BINARY}
        });
    }

    /*
     * Tailer doesn't work if created before the appender
     *
     * See https://higherfrequencytrading.atlassian.net/browse/QUEUE-28
     */

    @Test
    public void test() {
        File dir = getTmpDir();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.builder(dir, wireType)
                .testBlockSize()
                .build()) {

            final ExcerptTailer tailer = queue.createTailer();
            assertFalse(tailer.readDocument(r -> r.read(TestKey.test).int32()));

            final ExcerptAppender appender = queue.acquireAppender();
            appender.writeDocument(w -> w.write(TestKey.test).int32(1));
            Jvm.pause(100);
            assertTrue(tailer.readDocument(r -> r.read(TestKey.test).int32()));
        }
    }
}