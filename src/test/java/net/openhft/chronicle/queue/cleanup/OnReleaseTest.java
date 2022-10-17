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

package net.openhft.chronicle.queue.cleanup;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.testframework.FlakyTestRunner;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.MINUTELY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OnReleaseTest extends QueueTestCommon {
    @Test
    public void onRelease() throws Throwable {
        FlakyTestRunner.builder(this::onRelease0).build().run();
    }

    public void onRelease0() {
        String path = OS.getTarget() + "/onRelease-" + Time.uniqueId();
        SetTimeProvider stp = new SetTimeProvider();
        AtomicInteger writeRoll = new AtomicInteger();
        AtomicInteger readRoll = new AtomicInteger();
        try (ChronicleQueue writeQ = SingleChronicleQueueBuilder
                .binary(path)
                .rollCycle(MINUTELY)
                .timeProvider(stp)
                .storeFileListener((c, f) -> {
                    System.out.println("write released " + f);
                    writeRoll.incrementAndGet();
                })
                .build();
             ChronicleQueue readQ = SingleChronicleQueueBuilder
                     .binary(path)
                     .rollCycle(MINUTELY)
                     .timeProvider(stp)
                     .storeFileListener((c, f) -> {
                         System.out.println("read released " + f);
                         readRoll.incrementAndGet();
                     })
                     .build()) {
            ExcerptAppender appender = writeQ.acquireAppender();
            ExcerptTailer tailer = readQ.createTailer();
            for (int i = 0; i < 500; i++) {
                appender.writeText("hello-" + i);
                assertNotNull(tailer.readText());
                BackgroundResourceReleaser.releasePendingResources();
                assertEquals(i, writeRoll.get());
                assertEquals(i, readRoll.get());
                stp.advanceMillis(66_000);
            }
        }

    }
}
