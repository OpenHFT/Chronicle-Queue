/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.Pretoucher;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

/**
 * Verifies that ChronicleQueue#createPretoucher() returns a non-recursive implementation
 * in OSS builds and does not blow the stack. In enterprise builds this may be a functional
 * pretoucher; the test only asserts that no StackOverflowError occurs and creation succeeds.
 */
public class PretoucherCreationTest extends QueueTestCommon {

    @Test
    public void createPretoucherDoesNotRecurseOrThrow() {
        // Enterprise-only warning is expected in OSS environments
        ignoreException("Pretoucher is only supported");

        final File dir = getTmpDir();
        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(dir).build()) {
            final Pretoucher pretoucher = q.createPretoucher();
            assertNotNull(pretoucher);

            try {
                pretoucher.execute();
            } catch (net.openhft.chronicle.core.threads.InvalidEventHandlerException ignored) {
                // acceptable if implementation indicates closure
            } catch (StackOverflowError soe) {
                fail("Pretoucher execution recursed and overflowed the stack");
            }

            // No exceptions expected on close
            pretoucher.close();
        }
    }
}

