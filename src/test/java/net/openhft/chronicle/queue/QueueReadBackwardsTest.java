/*
 * Copyright 2014-2020 chronicle.software
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;

public class QueueReadBackwardsTest extends QueueTestCommon {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private File dataDir;
    private SetTimeProvider timeProvider;

    @Before
    public void setup() throws IOException {
        this.dataDir = temporaryFolder.newFolder();
        this.timeProvider = new SetTimeProvider(new Date().getTime());
    }

    @Test
    public void testReadBackwardsAfterWriteJustOneMessage() {
        RollCycles rollingCycle = RollCycles.DEFAULT;
        // Write a message to the queue
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dataDir)
                .timeProvider(timeProvider)
                .rollCycle(rollingCycle)
                .build()) {
            queue.acquireAppender().writeText("42");
        }

        // Wait less than the rolling cycle
        timeProvider.advanceMillis(TimeUnit.HOURS.toMillis(6));

        // Read backwards from the end
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dataDir)
                .timeProvider(timeProvider)
                .rollCycle(rollingCycle)
                .build()) {
            ExcerptTailer tailer = queue.createTailer().toEnd().direction(TailerDirection.BACKWARD);
            // An exception is thrown here
            String read = tailer.readText();
            assertEquals("42", read);
        }
 }
}
