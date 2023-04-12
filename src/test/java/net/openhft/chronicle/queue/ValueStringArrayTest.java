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

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class ValueStringArrayTest extends QueueTestCommon {

    private static final String EXPECTED = "hello world";
    private final ValueStringArray using = new ValueStringArray();

    @Test
    public void test() {
        // No explicit support of putting a Value into Wire.
        expectException("BytesMarshallable found in field which is not matching exactly");

        ValueStringArray value = new ValueStringArray();
        value.setCsArrItem(1, EXPECTED);

        // this is the directory the queue is written to
        final File dataDir = getTmpDir();

        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir).build()) {

            try (DocumentContext dc = queue.acquireAppender().writingDocument()) {
                dc.wire().write("data").marshallable(value);
            }

            try (DocumentContext dc = queue.createTailer().readingDocument()) {
                dc.wire().read("data").marshallable(using);
                CharSequence actual = using.getCsArr().getCharSequenceWrapperAt(1).getCharSequence();
               // System.out.println(actual);
                Assert.assertEquals(EXPECTED, actual.toString());
            }
         }
    }
}

