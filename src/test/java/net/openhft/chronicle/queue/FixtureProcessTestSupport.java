/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.Assert;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Runs a resource-order assertion with the deferred queue enabled and its consumer thread disabled. */
public final class FixtureProcessTestSupport {
    private FixtureProcessTestSupport() { }

    public static boolean runDeferredReleaseTest(Class<?> test, String method) throws Exception {
        if ("false".equals(System.getProperty("background.releaser.thread"))) {
            Assert.assertNotEquals("deferred release must remain enabled", "false",
                    System.getProperty("background.releaser"));
            return false;
        }
        List<String> command = new ArrayList<>();
        command.add(new File(System.getProperty("java.home"), "bin/java").toString());
        for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments())
            if (argument.startsWith("--add-") || argument.startsWith("-D")
                    || argument.equals("-ea"))
                command.add(argument);
        command.add("-Xms32m");
        command.add("-Xmx256m");
        command.add("-Dbackground.releaser=true");
        command.add("-Dbackground.releaser.thread=false");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(FixtureProcessTestSupport.class.getName());
        command.add(test.getName());
        command.add(method);
        Path output = Files.createTempFile("queue-deferred-release-", ".log");
        Throwable primary = null;
        try {
            // Native inheritIO bypasses Surefire's event stream. Capture output in an owned file.
            try (TestProcessCleanup owned = new TestProcessCleanup(new ProcessBuilder(command)
                    .redirectErrorStream(true).redirectOutput(output.toFile()).start(), 5, TimeUnit.SECONDS)) {
                Process child = owned.process();
                Assert.assertTrue("deferred release child completed; output=" + output,
                        child.waitFor(45, TimeUnit.SECONDS));
                Assert.assertEquals("deferred release assertion failed in child:\n"
                        + new String(Files.readAllBytes(output), StandardCharsets.UTF_8), 0, child.exitValue());
            }
        } catch (Exception | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            try {
                Files.deleteIfExists(output);
            } catch (Exception cleanupFailure) {
                if (primary == null)
                    throw cleanupFailure;
                primary.addSuppressed(cleanupFailure);
            }
        }
        return true;
    }

    public static void main(String[] args) throws Exception {
        Result result = new JUnitCore().run(Request.method(Class.forName(args[0]), args[1]));
        result.getFailures().forEach(failure -> System.err.println(failure.getTrace()));
        System.out.println("Deferred release child: runs=" + result.getRunCount()
                + " failures=" + result.getFailureCount() + " ignored=" + result.getIgnoreCount());
        if (result.getRunCount() != 1 || !result.wasSuccessful() || result.getIgnoreCount() != 0)
            System.exit(1);
    }
}
