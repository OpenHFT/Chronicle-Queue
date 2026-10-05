/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Map;
import static org.junit.Assert.*;

public class QueueTestCommonResourceTest {
    @Test
    public void boundariesAreOptInAndPreserveFailures() throws Throwable {
        QueueTestCommon fixture = new QueueTestCommon();
        Description description = Description.createTestDescription(getClass(), "boundaryProbe");
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        AssertionError primary = new AssertionError("primary test assertion");
        Map<ExceptionKey, Integer> recorded = Jvm.recordExceptions(false);
        try {
            System.setOut(new PrintStream(captured));
            fixture.watcher.apply(new Statement() {
                @Override public void evaluate() { }
            }, description).evaluate();
            try {
                fixture.watcher.apply(new Statement() {
                    @Override public void evaluate() { throw primary; }
                }, description).evaluate();
                fail("primary assertion was swallowed");
            } catch (AssertionError caught) {
                assertSame(primary, caught);
            }
            String output = captured.toString("UTF-8");
            if (Boolean.getBoolean("queue.traceTestExecution")) {
                assertEquals("two start boundaries", 2, occurrences(output, "QueueTestExecution phase=start"));
                assertEquals("two finish boundaries", 2, occurrences(output, "QueueTestExecution phase=finish"));
                for (String field : new String[]{" timeMs=", " pid=", " test=" + getClass().getName()
                        + ".boundaryProbe", " heapUsed=", " heapCommitted=", " heapMax=", " target="})
                    assertTrue("missing context " + field, output.contains(field));
            } else {
                assertEquals("diagnostics are opt in", "", output);
            }
            assertTrue("boundaries must not become recorded warnings/errors", recorded.isEmpty());
        } finally {
            System.setOut(original);
            Jvm.resetExceptionHandlers();
        }
    }

    @Test
    public void sharedDiskDropIsContextAndUnrelatedWarningsStillFail() throws Exception {
        QueueTestCommon fixture = new QueueTestCommon() {
            int reads;
            @Override long diskFreeSpace() { return (++reads == 1 ? 10L : 7L) << 30; }
        };
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        fixture.assumeFinishedNormally();
        fixture.recordExceptions();
        try {
            System.setOut(new PrintStream(captured));
            fixture.recordDiskSpace();
            fixture.checkSpaceUsed();
            String output = captured.toString("UTF-8");
            assertTrue("shared drop is visible", output.contains("Shared filesystem free space decreased by 3.0 GiB"));
            assertTrue("does not attribute usage", output.contains("this is not per-test disk usage"));
            fixture.exceptionTracker.checkExceptions();
            fixture.recordExceptions();
            Jvm.warn().on(getClass(), "unrelated resource contract warning");
            AssertionError failure = org.junit.Assert.assertThrows(AssertionError.class, fixture::afterChecks);
            assertTrue("unrelated warning still fails", failure.toString().contains("unrelated resource contract warning"));
        } finally {
            System.setOut(original);
            Jvm.resetExceptionHandlers();
        }
    }

    private static int occurrences(String text, String word) {
        return (text.length() - text.replace(word, "").length()) / word.length();
    }
}
