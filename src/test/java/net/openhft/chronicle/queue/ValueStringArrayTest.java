/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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

        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir).build();
             final ExcerptAppender excerptAppender = queue.createAppender()) {

            try (DocumentContext dc = excerptAppender.writingDocument()) {
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
