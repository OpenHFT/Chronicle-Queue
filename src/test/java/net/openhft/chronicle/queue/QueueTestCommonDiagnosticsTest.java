/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.wire.VanillaMethodWriterBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class QueueTestCommonDiagnosticsTest {
    private static final String FALLBACK =
            "Failed to compile generated method writer - falling back to proxy method writer";
    private static final Throwable COMPILER_FAILURE =
            new IllegalStateException("original compiler failure", new Exception("compiler failure cause"));
    private static final AssertionError ORIGINAL_FAILURE = new AssertionError("original fixture failure");
    private static ByteArrayOutputStream output;

    @Before
    public void captureDiagnosticOutput() {
        output = new ByteArrayOutputStream();
    }

    @After
    public void restoreExceptionHandlers() {
        // Deliberately failed and unfinished inner fixtures do not reset these handlers themselves.
        Jvm.resetExceptionHandlers();
    }

    @Test
    public void expectedWarningsStillPassAndReportFirstThrowableWithContextAndCount() {
        assertPasses(run(Fixtures.class, "expectedWarnings"));
        String diagnostic = diagnostic();
        assertTrue(diagnostic, diagnostic.contains("count=2"));
        assertTrue(diagnostic, diagnostic.contains("test=" + Fixtures.class.getName() + ".expectedWarnings"));
        assertTrue(diagnostic, diagnostic.contains("java.version=" + System.getProperty("java.version")));
        assertTrue(diagnostic, diagnostic.contains("wire.generator.v2=" + System.getProperty("wire.generator.v2")));
        assertTrue(diagnostic, diagnostic.contains("disableProxyCodegen=" + System.getProperty("disableProxyCodegen")));
        assertTrue(diagnostic, diagnostic.contains(FALLBACK));
        assertTrue(diagnostic, diagnostic.contains("java.lang.IllegalStateException: original compiler failure"));
        assertTrue(diagnostic, diagnostic.contains("Caused by: java.lang.Exception: compiler failure cause"));
        assertTrue(diagnostic, diagnostic.contains("\tat "));
        assertFalse(diagnostic, diagnostic.contains("second distinct compiler failure"));
        assertEquals(diagnostic, 1, diagnostic.split("Method writer fallback diagnostic", -1).length - 1);
    }

    @Test
    public void identifiesTestWhenSubclassDeclaresItsOwnTestNameRule() {
        assertPasses(run(ShadowedTestName.class, "expectedWarnings"));
        String identity = "test=" + ShadowedTestName.class.getName() + ".expectedWarnings";
        assertTrue(diagnostic(), diagnostic().contains(identity));
    }

    @Test
    public void unexpectedWarningStillFailsWithExactlyOneRecordedException() {
        Result result = run(Fixtures.class, "unexpectedWarning");
        assertEquals(result.getFailures().toString(), 1, result.getFailureCount());
        assertTrue(result.getFailures().toString(), result.getFailures().get(0).getMessage()
                .startsWith("1 exceptions were detected: " + FALLBACK));
        assertDiagnosticPresent();
    }

    @Test
    public void failingTestKeepsItsOriginalFailureAndReportsDiagnostic() {
        assertOriginalFailure(run(Fixtures.class, "failingTest"));
        assertDiagnosticPresent();
    }

    @Test
    public void failingCleanupKeepsItsOriginalFailureAndReportsDiagnostic() {
        assertOriginalFailure(run(FailingCleanup.class, "expectedWarnings"));
        assertDiagnosticPresent();
    }

    @Test
    public void failingResourceCheckKeepsItsOriginalFailureAndReportsDiagnostic() {
        assertOriginalFailure(run(FailingResourceCheck.class, "expectedWarnings"));
        assertDiagnosticPresent();
    }

    @Test
    public void unfinishedTestStillSkipsExceptionCheckingButReportsDiagnostic() {
        assertPasses(run(Fixtures.class, "unfinishedTest"));
        assertDiagnosticPresent();
    }

    @Test
    public void brokenSinkDoesNotFailAnOtherwisePassingTest() {
        assertPasses(run(BrokenSink.class, "expectedWarnings"));
    }

    @Test
    public void brokenSinkDoesNotReplaceTheOriginalFailure() {
        assertOriginalFailure(run(BrokenSink.class, "failingTest"));
    }

    @Test
    public void unrelatedWarningsProduceNoDiagnostic() {
        assertPasses(run(Fixtures.class, "unrelatedWarnings"));
        assertEquals("", diagnostic());
    }

    @Test
    public void testsWithoutWarningsProduceNoDiagnostic() {
        assertPasses(run(Fixtures.class, "noWarnings"));
        assertEquals("", diagnostic());
    }

    private static Result run(Class<? extends Fixtures> fixture, String method) {
        Result result = new JUnitCore().run(Request.method(fixture, method));
        assertEquals(result.getFailures().toString(), 1, result.getRunCount());
        return result;
    }

    private static void assertPasses(Result result) {
        assertEquals(result.getFailures().toString(), 0, result.getFailureCount());
    }

    private static void assertOriginalFailure(Result result) {
        assertEquals(result.getFailures().toString(), 1, result.getFailureCount());
        assertSame(ORIGINAL_FAILURE, result.getFailures().get(0).getException());
    }

    private static String diagnostic() {
        return new String(output.toByteArray(), StandardCharsets.UTF_8);
    }

    private static void assertDiagnosticPresent() {
        assertTrue(diagnostic(), diagnostic().contains("original compiler failure"));
    }

    public static class Fixtures extends QueueTestCommon {
        @Override
        PrintStream methodWriterDiagnosticStream() {
            return new PrintStream(output);
        }

        @Test
        public void expectedWarnings() {
            expectException(FALLBACK);
            warn(COMPILER_FAILURE);
            warn(COMPILER_FAILURE);
            warn(new IllegalArgumentException("second distinct compiler failure"));
        }

        @Test
        public void unexpectedWarning() {
            warn(COMPILER_FAILURE);
        }

        @Test
        public void failingTest() {
            expectException(FALLBACK);
            warn(COMPILER_FAILURE);
            throw ORIGINAL_FAILURE;
        }

        @Test
        public void unfinishedTest() {
            warn(COMPILER_FAILURE);
            finishedNormally = false;
        }

        @Test
        public void unrelatedWarnings() {
            expectException(FALLBACK);
            expectException("unrelated writer warning");
            Jvm.warn().on(getClass(), FALLBACK, COMPILER_FAILURE);
            Jvm.warn().on(VanillaMethodWriterBuilder.class, "unrelated writer warning", COMPILER_FAILURE);
        }

        @Test
        public void noWarnings() {
            // The default path must remain silent.
        }

        private void warn(Throwable failure) {
            Jvm.warn().on(VanillaMethodWriterBuilder.class, FALLBACK, failure);
        }
    }

    public static class ShadowedTestName extends Fixtures {
        // MessageHistoryTest also declares this rule, hiding QueueTestCommon.testName from JUnit.
        @Rule
        public final TestName testName = new TestName();
    }

    public static class FailingCleanup extends Fixtures {
        @Override
        protected void preAfter() {
            throw ORIGINAL_FAILURE;
        }
    }

    public static class FailingResourceCheck extends Fixtures {
        @Override
        public void assertReferencesReleased() {
            throw ORIGINAL_FAILURE;
        }
    }

    public static class BrokenSink extends Fixtures {
        @Override
        PrintStream methodWriterDiagnosticStream() {
            return new PrintStream(output) {
                @Override
                public void println(String message) {
                    throw new AssertionError("diagnostic sink failure");
                }
            };
        }
    }
}
