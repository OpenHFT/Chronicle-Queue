/*
 * Copyright 2016-2020 chronicle.software
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
import net.openhft.chronicle.queue.TestKey;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static net.openhft.chronicle.queue.impl.single.StoreTailer.INDEXING_LINEAR_SCAN_THRESHOLD;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class IndexTest extends ChronicleQueueTestBase {

    @NotNull
    private final WireType wireType;

    /**
     * @param wireType the type of the wire
     */
    public IndexTest(@NotNull WireType wireType) {
        this.wireType = wireType;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
               // {WireType.TEXT}, // TODO Add CAS to LongArrayReference.
                {WireType.BINARY}
        });
    }

    @Test
    public void test() throws IOException {

        try (final RollingChronicleQueue queue = SingleChronicleQueueBuilder
                .binary(getTmpDir())
                .testBlockSize()
                .wireType(this.wireType)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            for (int i = 0; i < 5; i++) {
                final int n = i;
                appender.writeDocument(
                        w -> w.write(TestKey.test).int32(n));
                final int cycle = queue.lastCycle();
                long index0 = queue.rollCycle().toIndex(cycle, n);
                long indexA = appender.lastIndexAppended();
                accessHexEquals(index0, indexA);
            }
        }
    }

    @Test
    public void shouldShortCircuitIndexLookupWhenNewIndexIsCloseToPreviousIndex() {
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder
                .binary(getTmpDir())
                .testBlockSize()
                .wireType(this.wireType)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();

            final int messageCount = INDEXING_LINEAR_SCAN_THRESHOLD + 5;
            final long[] indices = new long[messageCount];
            for (int i = 0; i < messageCount; i++) {
                try (final DocumentContext ctx = appender.writingDocument()) {
                    ctx.wire().write("event").int32(i);
                    indices[i] = ctx.index();
                }
            }

            final StoreTailer tailer =
                    (StoreTailer) queue.createTailer();
            tailer.moveToIndex(indices[0]);

            assertEquals(indices[0], tailer.index());
            assertEquals(1, tailer.getIndexMoveCount());

            tailer.moveToIndex(indices[0]);
            assertEquals(indices[0], tailer.index());
            assertEquals(1, tailer.getIndexMoveCount());

            tailer.moveToIndex(indices[2]);
            assertEquals(indices[2], tailer.index());
            assertEquals(1, tailer.getIndexMoveCount());

            tailer.moveToIndex(indices[INDEXING_LINEAR_SCAN_THRESHOLD + 2]);
            assertEquals(indices[INDEXING_LINEAR_SCAN_THRESHOLD + 2], tailer.index());
            assertEquals(2, tailer.getIndexMoveCount());

            // document that moving backwards requires an index scan
            tailer.moveToIndex(indices[INDEXING_LINEAR_SCAN_THRESHOLD - 1]);
            assertEquals(indices[INDEXING_LINEAR_SCAN_THRESHOLD - 1], tailer.index());
            assertEquals(3, tailer.getIndexMoveCount());
        }
    }

    public void accessHexEquals(long index0, long indexA) {
        assertEquals(Long.toHexString(index0) + " != " + Long.toHexString(indexA), index0, indexA);
    }
}
