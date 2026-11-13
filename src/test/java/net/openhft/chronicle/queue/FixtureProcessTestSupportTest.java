/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.Assert.*;

public class FixtureProcessTestSupportTest {
    @Test
    public void childOutputDoesNotBypassTheSurefireStream() throws Exception {
        PrintStream original = System.out;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try (PrintStream output = new PrintStream(captured)) {
            System.setOut(output);
            assertTrue(FixtureProcessTestSupport.runDeferredReleaseTest(ChildCase.class, "passes"));
            assertEquals("Child output must stay in its owned capture file", "", captured.toString());
        } finally {
            System.setOut(original);
        }
    }

    @Test
    public void childFailureIncludesItsCapturedOutput() {
        AssertionError failure = assertThrows(AssertionError.class,
                () -> FixtureProcessTestSupport.runDeferredReleaseTest(ChildCase.class, "fails"));
        assertTrue(failure.getMessage(), failure.getMessage().contains("deliberate-child-output"));
        assertTrue(failure.getMessage(), failure.getMessage().contains("deliberate-child-failure"));
    }

    public static class ChildCase {
        @Test
        public void passes() {
            System.out.println("deliberate-child-output");
            assertEquals("false", System.getProperty("background.releaser.thread"));
        }

        @Test
        public void fails() {
            System.err.println("deliberate-child-output");
            fail("deliberate-child-failure");
        }
    }
}
