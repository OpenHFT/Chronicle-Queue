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
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;

public class TestCallingToEndOnRoll extends QueueTestCommon implements TimeProvider {

    private long currentTime = 0;
    private ExcerptAppender appender;
    private ExcerptTailer tailer;

    @Ignore("long running soak test to check https://github.com/OpenHFT/Chronicle-Queue/issues/702")
    @Test
    public void test() {
        SingleChronicleQueue build = binary(getTmpDir()).rollCycle(TEST_SECONDLY).timeProvider(this).build();
        appender = build.acquireAppender();

        tailer = build.createTailer();
        Executors.newSingleThreadExecutor().submit(this::append);

        Executors.newSingleThreadExecutor().submit(this::toEnd);
        LockSupport.park();
    }

    private void append() {
        for (; ; ) {
            toEnd0();
            appender.writeText("hello world");
            toEnd0();
        }
    }

    private void toEnd() {
        for (; ; ) {
            toEnd0();
        }
    }

    private void toEnd0() {
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
