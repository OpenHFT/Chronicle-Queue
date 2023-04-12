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
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;

import java.io.File;
import java.util.Arrays;

/**
 * see https://github.com/OpenHFT/Chronicle-Queue/issues/535
 * Created by Rob Austin
 */
public class MappedFileSafeLimitTooSmallTest extends QueueTestCommon {

    @org.junit.Test
    public void testMappedFileSafeLimitTooSmall() {

        final int arraySize = 40_000;
        final int blockSize = arraySize * 6;
        byte[] data = new byte[arraySize];
        Arrays.fill(data, (byte) 'x');
        File tmpDir = getTmpDir();

        try (final ChronicleQueue queue =
                     SingleChronicleQueueBuilder.builder(tmpDir, WireType.BINARY).blockSize(blockSize).build()) {

            for (int i = 0; i < 5; i++) {
                try (DocumentContext dc = queue.acquireAppender().writingDocument()) {
                   // System.out.println(dc.wire().bytes().writeRemaining());
                    dc.wire().write("data").bytes(data);
                }
            }
        }

        try (final ChronicleQueue queue =
                     SingleChronicleQueueBuilder.builder(tmpDir, WireType.BINARY).blockSize(blockSize).build()) {

            for (int i = 0; i < 5; i++) {
                try (DocumentContext dc = queue.createTailer().readingDocument()) {
                    Assert.assertArrayEquals(data, dc.wire().read("data").bytes());
                }
            }
 }
    }
}
