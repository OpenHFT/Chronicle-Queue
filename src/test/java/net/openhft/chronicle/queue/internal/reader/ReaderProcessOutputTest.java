/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.testframework.process.JavaProcessBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;

public class ReaderProcessOutputTest {
    @Test(timeout = 20_000)
    public void drainsBothPipesBeforeWaitingForExit() throws Exception {
        Process process = JavaProcessBuilder.create(NoisyChild.class).start();
        String output = ReaderProcessOutput.readAndClose(process);
        assertTrue(output, output.endsWith("reader-finished"));
        assertFalse(process.isAlive());
    }

    @Test(timeout = 20_000)
    public void interruptedWaitStillTerminatesOwnedChild() throws Exception {
        Process process = JavaProcessBuilder.create(BlockedChild.class).start();
        Thread.currentThread().interrupt();
        try {
            assertThrows(InterruptedException.class, () -> ReaderProcessOutput.readAndClose(process));
            assertTrue("Cleanup must restore interruption", Thread.currentThread().isInterrupted());
            assertFalse("Interrupted test left its reader process alive", process.isAlive());
        } finally {
            Thread.interrupted();
            if (process.isAlive())
                process.destroyForcibly();
        }
    }

    public static class NoisyChild {
        public static void main(String[] args) {
            char[] data = new char[8192];
            Arrays.fill(data, 'x');
            for (int i = 0; i < 128; i++) {
                System.out.print(data);
                System.err.print(data);
            }
            System.out.print("reader-finished");
        }
    }

    public static class BlockedChild {
        public static void main(String[] args) throws InterruptedException {
            new CountDownLatch(1).await();
        }
    }
}
