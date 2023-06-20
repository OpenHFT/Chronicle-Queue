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

import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.junit.*;

import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;

public class TestCallingToEndOnRoll extends QueueTestCommon implements TimeProvider {

    private long currentTime = 0;
    private SingleChronicleQueue queue;

    @Before
    public void setUp() {
        queue = binary(getTmpDir()).rollCycle(TEST_SECONDLY).timeProvider(this).build();
    }

    @Override
    @After
    public void tearDown() {
        closeQuietly(queue);
    }

    @Ignore("long running soak test to check https://github.com/OpenHFT/Chronicle-Queue/issues/702")
    @Test
    public void test() {
        Executors.newSingleThreadExecutor().submit(this::append);

        Executors.newSingleThreadExecutor().submit(this::toEnd);
        LockSupport.park();
    }

    private void append() {
        try (final ExcerptAppender appender = queue.createAppender();
             final ExcerptTailer tailer = queue.createTailer()) {
            for (; ; ) {
                toEnd0(tailer);
                appender.writeText("hello world");
                toEnd0(tailer);
            }
        }
    }

    private void toEnd() {
        try (final ExcerptTailer tailer = queue.createTailer()) {
            for (; ; ) {
                toEnd0(tailer);
            }
        }
    }

    private void toEnd0(ExcerptTailer tailer) {
        try {
            long index = tailer.toEnd().index();
            // System.out.println("index = " + index);
        } catch (IllegalStateException e) {
            e.printStackTrace();
            Assert.fail();
            System.exit(-1);
        }
    }

    @Override
    public long currentTimeMillis() {
        return (currentTime = currentTime + 1000);
    }
}
