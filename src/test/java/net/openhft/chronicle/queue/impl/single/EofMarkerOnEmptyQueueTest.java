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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.ReferenceOwner;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wires;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public final class EofMarkerOnEmptyQueueTest extends QueueTestCommon {
    private static final ReferenceOwner test = ReferenceOwner.temporary("test");
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Test
    public void shouldRecoverFromEmptyQueueOnRoll() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        Assume.assumeFalse(OS.isWindows());
        System.setProperty("queue.force.unlock.mode", "ALWAYS");
        expectException("Couldn't acquire write lock");
        expectException("Forced unlock for the lock");

        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        try (final RollingChronicleQueue queue =
                     ChronicleQueue.singleBuilder(tmpFolder.newFolder()).
                             rollCycle(TEST_SECONDLY).
                             timeProvider(clock::get).
                             timeoutMS(1_000).
                             testBlockSize().build()) {
            final ExcerptAppender appender = queue.acquireAppender();
            final DocumentContext context = appender.writingDocument();
            // start to write a message, but don't close the context - simulates crashed writer
            final long expectedEofMarkerPosition = context.wire().bytes().writePosition() - Wires.SPB_HEADER_SIZE;
            context.wire().write("foo").int32(1);
            final int startCycle = queue.cycle();

            clock.addAndGet(TimeUnit.SECONDS.toMillis(1L));

            final int nextCycle = queue.cycle();

            // ensure that the cycle file will roll
            assertNotEquals(nextCycle, startCycle);

            ExecutorService appenderExecutor = Executors.newSingleThreadExecutor(
                    new NamedThreadFactory("Appender"));
            appenderExecutor.submit(() -> {
                try (final DocumentContext nextCtx = queue.acquireAppender().writingDocument()) {
                    nextCtx.wire().write("bar").int32(7);
                }
            }).get(Jvm.isDebug() ? 3000 : 3, TimeUnit.SECONDS);

            appenderExecutor.shutdown();
            appenderExecutor.awaitTermination(1, TimeUnit.SECONDS);

            try (final SingleChronicleQueueStore firstCycleStore = queue.storeForCycle(startCycle, 0, false, null)) {

                final long firstCycleWritePosition = firstCycleStore.writePosition();
                // assert that no write was completed
                assertEquals(0L, firstCycleWritePosition);
                // firstCycleStore.release(test);

                final ExcerptTailer tailer = queue.createTailer();
                int recordCount = 0;
                int lastItem = -1;
                while (true) {
                    try (final DocumentContext readCtx = tailer.readingDocument()) {
                        if (!readCtx.isPresent()) {
                            break;
                        }

                        final StringBuilder name = new StringBuilder();
                        final ValueIn field = readCtx.wire().readEventName(name);
                        recordCount++;
                        lastItem = field.int32();
                    }
                }
                assertEquals(1, recordCount);
                assertEquals(7, lastItem);
            }
        } finally {
            System.clearProperty("queue.force.unlock.mode");
        }
    }
}