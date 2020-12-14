/*
 * Copyright 2016-2020 chronicle.software
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.ReadMarshallable;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

import static java.lang.System.currentTimeMillis;
import static net.openhft.chronicle.queue.RollCycles.HOURLY;

@RequiredForClient
public class MoveIndexAfterFailedTailerTest extends QueueTestCommon {
    private static final Logger LOGGER = LoggerFactory.getLogger(MoveIndexAfterFailedTailerTest.class);

    @Test
    public void test() {
        String basePath = OS.getTarget() + "/" + getClass().getSimpleName() + "-" + Time.uniqueId();
        final SingleChronicleQueueBuilder myBuilder = SingleChronicleQueueBuilder.single(basePath)
                .testBlockSize()
                .timeProvider(System::currentTimeMillis)
                .rollCycle(HOURLY);

        int messages = 10;
        try (final ChronicleQueue myWrite = myBuilder.build()) {
            write(myWrite, messages);
           // System.out.println(myWrite.dump());
        }

        try (final ChronicleQueue myRead = myBuilder.build()) {
            read(myRead, messages);
        }
    }

    private void read(@NotNull ChronicleQueue aChronicle, int expected) {
        final ExcerptTailer myTailer = aChronicle.createTailer();
        final int myLast = HOURLY.toCycle(myTailer.toEnd().index());
        final int myFirst = HOURLY.toCycle(myTailer.toStart().index());
        int myCycle = myFirst - 1;
        long myIndex = HOURLY.toIndex(myCycle, 0);
        int count = 0;
        while (myCycle <= myLast) {
           // System.out.println(Long.toHexString(myIndex));
            if (myTailer.moveToIndex(myIndex)) {
                while (myTailer.readDocument(read())) {
                    count++;
                }
            }
            myIndex = HOURLY.toIndex(++myCycle, 0);
        }
        Assert.assertEquals(expected, count);
    }

    private ReadMarshallable read() {
        return aMarshallable -> {
            final byte[] myBytes = aMarshallable.read().bytes();
            if (myBytes != null) {
                LOGGER.info("Reading: {}", new String(myBytes, StandardCharsets.UTF_8));
            }
        };
    }

    private void write(@NotNull ChronicleQueue aChronicle, int messages) {
        final ExcerptAppender myAppender = aChronicle.acquireAppender();
        for (int myCount = 0; myCount < messages; myCount++) {
            myAppender.writeDocument(aMarshallable -> aMarshallable.write().bytes(Long.toString(currentTimeMillis()).getBytes(StandardCharsets.UTF_8)));
           // System.out.println(Long.toHexString(myAppender.lastIndexAppended()));
        }
    }
}
