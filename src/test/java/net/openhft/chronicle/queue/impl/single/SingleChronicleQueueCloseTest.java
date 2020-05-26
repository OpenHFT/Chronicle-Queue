/*
 * Copyright 2016-2020 Chronicle Software
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
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;
import org.junit.Test;

public class SingleChronicleQueueCloseTest extends ChronicleQueueTestBase {

    @Test
    public void testTailAfterClose() {
        final ChronicleQueue queue =
                SingleChronicleQueueBuilder.builder(getTmpDir(), WireType.BINARY).
                        build();
        final ExcerptAppender appender = queue.acquireAppender();
        appender.writeDocument(w -> w.write(TestKey.test).int32(1));
        queue.close();
        try {
            appender.writeDocument(w -> w.write(TestKey.test).int32(2));
            Assert.fail();
        } catch (IllegalStateException e) {
            // ok
        }
    }
}
