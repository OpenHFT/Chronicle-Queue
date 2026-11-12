/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;

/** Separate process kept alive at an explicit maintenance-inspection barrier. */
public final class TruncationWriterProcess {
    private TruncationWriterProcess() {
    }

    public static void main(String[] args) throws Exception {
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(args[0]).testBlockSize()
                .rollCycle(TestRollCycles.TEST_DAILY).timeProvider(new SetTimeProvider(86400000L)).build()) {
            ExcerptAppender appender = queue.createAppender();
            appender.writeText("first");
            System.out.println("READY");
            if (System.in.read() != 1)
                throw new IllegalStateException("Missing inspection barrier release");
            appender.writeText("second");
            System.out.println("APPENDED");
        }
    }
}
