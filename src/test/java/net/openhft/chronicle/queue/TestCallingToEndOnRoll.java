/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.jupiter.api.Assertions.*;

public class TestCallingToEndOnRoll extends QueueTestCommon implements TimeProvider {

    private long currentTime = 0;
    private SingleChronicleQueue queue;

    @BeforeEach
    public void setUp() {
        queue = binary(getTmpDir()).rollCycle(TEST_SECONDLY).timeProvider(this).build();
    }

    @Override
    @AfterEach
    public void tearDown() {
        closeQuietly(queue);
    }

    @Disabled("long running soak test to check https://github.com/OpenHFT/Chronicle-Queue/issues/702")
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
            tailer.toEnd().index();
        } catch (IllegalStateException e) {
            throw new AssertionError("tailer.toEnd() should not throw during the soak run", e);
        }
    }

    @Override
    public long currentTimeMillis() {
        return (currentTime = currentTime + 1000);
    }
}
