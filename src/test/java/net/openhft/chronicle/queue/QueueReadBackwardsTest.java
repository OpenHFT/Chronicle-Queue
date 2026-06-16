/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.SetTimeProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class QueueReadBackwardsTest extends QueueTestCommon {
    @TempDir
    Path temporaryFolder;

    private File dataDir;
    private SetTimeProvider timeProvider;

    @BeforeEach
    void setup() throws IOException {
        this.dataDir = Files.createTempDirectory(temporaryFolder, "queue").toFile();
        this.timeProvider = new SetTimeProvider(new Date().getTime());
    }

    @Test
    void testReadBackwardsAfterWriteJustOneMessage() {
        RollCycles rollingCycle = RollCycles.DEFAULT;
        // Write a message to the queue
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dataDir)
                .timeProvider(timeProvider)
                .rollCycle(rollingCycle)
                .build();
             final ExcerptAppender excerptAppender = queue.createAppender()) {
            excerptAppender.writeText("42");
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
