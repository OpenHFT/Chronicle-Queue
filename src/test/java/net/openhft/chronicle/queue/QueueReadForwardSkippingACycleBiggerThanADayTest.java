/*
 * Copyright 2014-2018 Chronicle Software
 *
 * http://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.harness.WeeklyRollCycle;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

public class QueueReadForwardSkippingACycleBiggerThanADayTest extends QueueTestCommon {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private File dataDir;
    private SetTimeProvider timeProvider;

    @Before
    public void setup() throws IOException {
        this.dataDir = temporaryFolder.newFolder();
        this.timeProvider = new SetTimeProvider();
    }

    @Test
    public void testReadForwards() {
        RollCycle rollingCycle = WeeklyRollCycle.INSTANCE;
        // Write a message to the queue
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dataDir)
                .timeProvider(timeProvider)
                .rollCycle(rollingCycle)
                .build()) {
            queue.acquireAppender().writeText("0");
        }

        // Wait more than a cycle
        timeProvider.advanceMillis(TimeUnit.DAYS.toMillis(18));

        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dataDir)
                .timeProvider(timeProvider)
                .rollCycle(rollingCycle)
                .build()) {
            queue.acquireAppender().writeText("42");
        }

        // Read backwards from the end
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dataDir)
                .timeProvider(timeProvider)
                .rollCycle(rollingCycle)
                .build()) {
            ExcerptTailer tailer = queue.createTailer().toStart().direction(TailerDirection.FORWARD);
            // An exception is thrown here
            assertEquals("0", tailer.readText());
            assertEquals("42", tailer.readText());
            assertNull(tailer.readText());
        }

    }

}
