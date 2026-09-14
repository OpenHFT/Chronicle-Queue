/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.testframework.process.JavaProcessBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class ReaderProcessOutputTest {
    @Test
    public void drainsBothPipesBeforeWaitingForExit() throws Exception {
        withChild("noisy", process -> {
            String output = ReaderProcessOutput.read(process, 5, TimeUnit.SECONDS);
            assertEquals(256 * 1024 + "stdout complete".length(), output.length());
            assertTrue(output.endsWith("stdout complete"));
            assertFalse(process.isAlive());
        });
    }

    @Test
    public void reportsNonzeroExitAndStderrEvenWhenStdoutMatches() throws Exception {
        withChild("fail", process -> {
            AssertionError failure = assertThrows(AssertionError.class,
                    () -> ReaderProcessOutput.read(process, 5, TimeUnit.SECONDS));
            assertTrue(failure.getMessage(), failure.getMessage().contains("child failure"));
            assertTrue(failure.getMessage(), failure.getMessage().contains("7"));
            assertFalse(process.isAlive());
        });
    }

    @Test
    public void timeoutTerminatesTheChildBeforeReturning() throws Exception {
        withChild("hang", process -> {
            AssertionError failure = assertThrows(AssertionError.class,
                    () -> ReaderProcessOutput.read(process, 100, TimeUnit.MILLISECONDS));
            assertTrue(failure.getMessage(), failure.getMessage().contains("did not exit within 100 ms"));
            assertFalse("Timed-out reader is still alive", process.isAlive());
        });
    }

    @Test
    public void interruptionTerminatesTheChildAndPreservesInterrupt() throws Exception {
        withChild("hang", process -> {
            Thread.currentThread().interrupt();
            try {
                assertThrows(InterruptedException.class,
                        () -> ReaderProcessOutput.read(process, 5, TimeUnit.SECONDS));
                assertTrue(Thread.currentThread().isInterrupted());
                assertFalse("Interrupted reader is still alive", process.isAlive());
            } finally {
                Thread.interrupted();
            }
        });
    }

    private static void withChild(String mode, ProcessCheck check) throws Exception {
        Process process = JavaProcessBuilder.create(Child.class).withProgramArguments(mode).start();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Void> future = executor.submit((Callable<Void>) () -> {
            check.accept(process);
            return null;
        });
        try {
            // An independent guard also bounds the negative control with the old unbounded helper.
            future.get(8, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Error)
                throw (Error) e.getCause();
            throw (Exception) e.getCause();
        } catch (TimeoutException e) {
            throw new AssertionError("Subprocess output helper did not complete", e);
        } finally {
            process.destroyForcibly();
            assertTrue("Test child did not terminate", process.waitFor(2, TimeUnit.SECONDS));
            executor.shutdownNow();
            assertTrue("Test worker did not terminate", executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @FunctionalInterface
    private interface ProcessCheck {
        void accept(Process process) throws Exception;
    }

    public static class Child {
        public static void main(String[] args) throws Exception {
            if ("hang".equals(args[0])) {
                System.in.read();
            } else if ("fail".equals(args[0])) {
                System.out.print("stdout complete");
                System.err.print("child failure");
                System.exit(7);
            } else {
                byte[] buffer = new byte[8192];
                Arrays.fill(buffer, (byte) 'x');
                for (int i = 0; i < 32; i++) {
                    System.out.write(buffer);
                    System.err.write(buffer);
                }
                System.out.print("stdout complete");
                System.err.print("stderr complete");
            }
        }
    }
}
