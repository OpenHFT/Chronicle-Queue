/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import java.nio.file.Files;

import static org.junit.Assert.*;

public class QueueTestCommonDiskSpaceTest {
    @Test
    public void sharedDriveGrowthDoesNotFailAnUnrelatedFixture() {
        Result result = JUnitCore.runClasses(SmallFixture.class);
        assertEquals(result.getFailures().toString(), 0, result.getFailureCount());
        assertEquals(1, result.getRunCount());
    }

    @Test
    public void sharedDriveGrowthDoesNotObscureTheTestFailure() {
        Result result = JUnitCore.runClasses(FailingFixture.class);
        assertEquals(result.getFailures().toString(), 1, result.getFailureCount());
        assertSame(FailingFixture.FAILURE, result.getFailures().get(0).getException());
    }

    public static class SmallFixture extends QueueTestCommon {
        private int measurements;

        @Override
        long diskFreeSpace() {
            // Model another process consuming 3 GiB without actually filling the drive.
            return (++measurements == 1 ? 8L : 5L) << 30;
        }

        @Test
        public void writesOnlyOneByte() throws Exception {
            // The fixture's own allocation is independent of the drive-wide change.
            Files.write(Files.createDirectories(getTmpDir().toPath()).resolve("owned.txt"), new byte[]{42});
        }
    }

    public static class FailingFixture extends QueueTestCommon {
        static final AssertionError FAILURE = new AssertionError("original test failure");
        private int measurements;

        @Override
        long diskFreeSpace() {
            return (++measurements == 1 ? 8L : 5L) << 30;
        }

        @Test
        public void failsInTheTestBody() {
            throw FAILURE;
        }
    }
}
