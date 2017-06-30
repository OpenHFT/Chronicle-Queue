/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
@RunWith(Parameterized.class)
public class IndexTest extends ChronicleQueueTestBase {

    @NotNull
    private final WireType wireType;
    private ThreadDump threadDump;

    /**
     * @param wireType the type of the wire
     */
    public IndexTest(@NotNull WireType wireType) {
        this.wireType = wireType;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
//                {WireType.TEXT}, // TODO Add CAS to LongArrayReference.
                {WireType.BINARY}
        });
    }

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
        threadDump.ignore(StoreComponentReferenceHandler.THREAD_NAME);
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @After
    public void checkMappedFiles() {
        MappedFile.checkMappedFiles();
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

    public void accessHexEquals(long index0, long indexA) {
        assertEquals(Long.toHexString(index0) + " != " + Long.toHexString(indexA), index0, indexA);
    }
}
