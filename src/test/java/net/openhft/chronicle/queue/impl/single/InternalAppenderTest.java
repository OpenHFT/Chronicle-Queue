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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

public class InternalAppenderTest extends QueueTestCommon {

    @Test
    public void replicationTest() throws Exception {
        final File file = Files.createTempDirectory("queue").toFile();
        try (final SingleChronicleQueue queue =
                     SingleChronicleQueueBuilder.single(file).build();
             final InternalAppender appender = (InternalAppender) queue.createAppender()) {
            final long index = queue.rollCycle().toIndex(queue.cycle(), 0);

            // First, we "replicate" a message, using the InternalAppender
            // interface because we need to preserve index numbers.
            appender.writeBytes(index, Bytes.from("Replicated"));

            // Next, a message is written locally by another app (usually a different process).
            try (final SingleChronicleQueue app = SingleChronicleQueueBuilder.single(file).build();
                 final ExcerptAppender appAppender = app.createAppender()) {
                appAppender.writeBytes(Bytes.from("Written locally"));
            }

            // The other app exits, and at some point later we need to start replicating again.

            appender.writeBytes(index + 2, Bytes.from("Replicated 2"));

            // We should have three messages in our queue.
            assertEquals(3, queue.entryCount());

        } finally {
            IOTools.deleteDirWithFiles(file);
        }
    }
}
