/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.ReadMarshallable;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static java.lang.System.currentTimeMillis;
import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.HOURLY;

@RequiredForClient
public class MoveIndexAfterFailedTailerTest extends QueueTestCommon {

    @Test
    @DisplayName("Reads all documents after failed tailer move")
    public void test() {
        String basePath = OS.getTarget() + "/" + getClass().getSimpleName() + "-" + Time.uniqueId();
        final SingleChronicleQueueBuilder myBuilder = SingleChronicleQueueBuilder.single(basePath)
                .testBlockSize()
                .timeProvider(System::currentTimeMillis)
                .rollCycle(HOURLY);

        int messages = 10;
        try (final ChronicleQueue myWrite = myBuilder.build();
             final ExcerptAppender appender = myWrite.createAppender()) {
            write(appender, messages);
        }

        try (final ChronicleQueue myRead = myBuilder.build()) {
            int readCount = read(myRead);
            Assertions.assertEquals(messages, readCount, "move-index: documents read");
        } finally {
            IOTools.deleteDirWithFiles(basePath);
        }
    }

    private int read(@NotNull ChronicleQueue aChronicle) {
        final ExcerptTailer myTailer = aChronicle.createTailer();
        final int myLast = HOURLY.toCycle(myTailer.toEnd().index());
        final int myFirst = HOURLY.toCycle(myTailer.toStart().index());
        int myCycle = myFirst - 1;
        long myIndex = HOURLY.toIndex(myCycle, 0);
        int count = 0;
        while (myCycle <= myLast) {
            if (myTailer.moveToIndex(myIndex)) {
                while (myTailer.readDocument(read())) {
                    count++;
                }
            }
            myIndex = HOURLY.toIndex(++myCycle, 0);
        }
        return count;
    }

    private ReadMarshallable read() {
        return aMarshallable -> {
            final byte[] myBytes = aMarshallable.read().bytes();
            if (myBytes != null) {
                Jvm.debug().on(getClass(), "Reading: " + new String(myBytes, StandardCharsets.UTF_8));
            }
        };
    }

    private void write(@NotNull ExcerptAppender myAppender, int messages) {
        for (int myCount = 0; myCount < messages; myCount++) {
            myAppender.writeDocument(aMarshallable -> aMarshallable.write().bytes(Long.toString(currentTimeMillis()).getBytes(StandardCharsets.UTF_8)));
        }
    }
}
