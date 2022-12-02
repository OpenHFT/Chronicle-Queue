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

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.testframework.GcControls;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.fail;

public final class MappedMemoryUnmappingTest extends QueueTestCommon {
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void shouldUnmapMemoryAsCycleRolls() throws IOException {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        long initialQueueMappedMemory = 0L;

        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.
                binary(tmp.newFolder()).testBlockSize().rollCycle(TEST_SECONDLY).
                timeProvider(clock::get).build()) {
            for (int i = 0; i < 100; i++) {
                queue.acquireAppender().writeDocument(System.nanoTime(), (d, t) -> d.int64(t));
                clock.addAndGet(TimeUnit.SECONDS.toMillis(1L));
                if (initialQueueMappedMemory == 0L) {
                    initialQueueMappedMemory = OS.memoryMapped();
                }
            }
        }

        GcControls.waitForGcCycle();

        final long timeoutAt = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L);
        while (System.currentTimeMillis() < timeoutAt) {
            if (OS.memoryMapped() < 2 * initialQueueMappedMemory) {
                return;
            }
        }

        fail(String.format("Mapped memory (%dB) did not fall below threshold (%dB)",
                OS.memoryMapped(), 2 * initialQueueMappedMemory));
    }
}
