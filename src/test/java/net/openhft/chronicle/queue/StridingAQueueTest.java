/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Mocker;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import java.io.File;
import java.io.StringWriter;

import static org.junit.Assert.assertEquals;

public class StridingAQueueTest extends ChronicleQueueTestBase {
    interface SAQMessage {
        void hi(int j, int i);
    }

    @Test
    public void testStriding() {
        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.currentTimeMillis(System.currentTimeMillis());
        File tmpDir = getTmpDir();
        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tmpDir).timeProvider(timeProvider).rollCycle(RollCycles.TEST_SECONDLY).build();
        SAQMessage writer = queue.acquireAppender().methodWriter(SAQMessage.class);
        for (int j = 1; j <= 5; j++) {
            for (int i = 0; i < 10 + j; i++)
                writer.hi(j, i);
            timeProvider.advanceMillis(j * 500);
        }
        StringWriter sw = new StringWriter();
        ExcerptTailer tailer = queue.createTailer().direction(TailerDirection.BACKWARD).toEnd().striding(true);
        MethodReader reader = tailer.methodReader(Mocker.logging(SAQMessage.class, "", sw));
        while(reader.readOne());
        assertEquals("hi[5, 14]\n" +
                "hi[5, 12]\n" +
                "hi[5, 8]\n" +
                "hi[5, 4]\n" +
                "hi[5, 0]\n" +
                "hi[4, 13]\n" +
                "hi[4, 12]\n" +
                "hi[4, 8]\n" +
                "hi[4, 4]\n" +
                "hi[4, 0]\n" +
                "hi[3, 12]\n" +
                "hi[3, 8]\n" +
                "hi[3, 4]\n" +
                "hi[3, 0]\n" +
                "hi[2, 11]\n" +
                "hi[2, 8]\n" +
                "hi[2, 4]\n" +
                "hi[2, 0]\n" +
                "hi[1, 10]\n" +
                "hi[1, 8]\n" +
                "hi[1, 4]\n" +
                "hi[1, 0]\n", sw.toString());
    }
}
