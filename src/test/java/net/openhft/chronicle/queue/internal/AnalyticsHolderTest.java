/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal;

import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.testframework.process.JavaProcessBuilder;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

public class AnalyticsHolderTest extends QueueTestCommon {

    @Test
    public void analyticsDebugMessagesDoNotReachStandardOutput() throws InterruptedException {
        assumeTrue("Chronicle Analytics is not on the classpath", isRealAnalyticsPresent());
        // An empty home directory makes Chronicle Analytics report the missing client id file to its debug logger
        // while the facade is built; no event is sent
        final File home = getTmpDir();
        assertTrue(home.mkdirs() || home.isDirectory());
        // Use Surefire's expanded classpath: java.class.path may only name a manifest launcher JAR.
        // Filtering that JAR leaves JUnit and Core's test stub reachable through its manifest.
        // Chronicle Analytics mutes itself when JUnit is visible.
        final String[] productionClasspath = Arrays.stream(System.getProperty("surefire.test.class.path",
                        System.getProperty("java.class.path")).split(File.pathSeparator))
                .filter(entry -> !new File(entry).getName().startsWith("junit") && !entry.endsWith("-tests.jar"))
                .toArray(String[]::new);

        final Process process = JavaProcessBuilder.create(AnalyticsDebugOutputMain.class)
                .withJvmArguments("-Dchronicle.analytics.disable=false", "-Duser.home=" + home.getAbsolutePath())
                .withClasspathEntries(productionClasspath)
                .start();
        // Drain both pipes while waiting: a chatty child cannot fill a pipe and block, and a hung child is killed
        final ByteArrayOutputStream stdoutBytes = new ByteArrayOutputStream();
        final ByteArrayOutputStream stderrBytes = new ByteArrayOutputStream();
        final Thread stdoutDrain = drain(process.getInputStream(), stdoutBytes);
        final Thread stderrDrain = drain(process.getErrorStream(), stderrBytes);
        final boolean exited = process.waitFor(30, TimeUnit.SECONDS);
        if (!exited)
            process.destroyForcibly();
        stdoutDrain.join(TimeUnit.SECONDS.toMillis(5));
        stderrDrain.join(TimeUnit.SECONDS.toMillis(5));
        final String stdout = new String(stdoutBytes.toByteArray(), StandardCharsets.UTF_8);
        final String stderr = new String(stderrBytes.toByteArray(), StandardCharsets.UTF_8);
        assertTrue("child process timed out:\n" + stdout + stderr, exited);
        assertEquals(stderr, 0, process.exitValue());

        assertFalse("analytics debug output on stdout:\n" + stdout, stdout.contains("Client id file not present"));
        assertTrue("analytics debug output was not routed to Jvm.debug():\n" + stdout + stderr,
                stdout.contains(AnalyticsDebugOutputMain.ROUTED + "true"));
    }

    private static boolean isRealAnalyticsPresent() {
        // looked up as a resource: loading it here would clash with the test-jar stub
        return AnalyticsHolderTest.class.getClassLoader()
                .getResource("net/openhft/chronicle/analytics/internal/VanillaAnalyticsBuilder.class") != null;
    }

    private static Thread drain(InputStream in, ByteArrayOutputStream out) {
        final Thread thread = new Thread(() -> {
            final byte[] buffer = new byte[4096];
            try {
                for (int n; (n = in.read(buffer)) >= 0; )
                    out.write(buffer, 0, n);
            } catch (IOException ignored) {
                // the stream closed when the child was destroyed; keep what was read
            }
        }, "analytics-child-drain");
        thread.setDaemon(true);
        thread.start();
        return thread;
    }
}
