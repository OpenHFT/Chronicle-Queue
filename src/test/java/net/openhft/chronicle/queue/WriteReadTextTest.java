/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import static org.junit.Assume.assumeTrue;

@RequiredForClient
public class WriteReadTextTest extends QueueTestCommon {

    private static final String CONSTRUCTED = "[\"abc\",\"comm_link\"," + "[[1469743199691,1469743199691],"
            + "[\"ABCDEFXH\",\"ABCDEFXH\"]," + "[321,456]," + "[\"\",\"\"]]]";
    @NotNull
    private static final String EXTREMELY_LARGE;
    private static final String MINIMAL = "[\"abc\"]";
    private static final String REALISTIC = "" +
            "[\"abc\",\"comm_link\",[[1469743199691,1469743199691],"
            + "[\"ABCDEFXH\",\"ABCDEFXH\"],"
            + "[321,456],"
            + "[-1408156298,-841885387],"
            + "[12345,9876],"
            + "[-841885387,-1408156298],"
            + "[9876,12345],"
            + "[0,0],"
            + "[\"FIX.4.2\",\"FIX.4.2\"],"
            + "[243,324],"
            + "[\"NewOrderSingle\",\"ExecutionReport\"],"
            + "[12862,13622],"
            + "[\"Q1W2E3R4T5Y6U7I8O9P0\",\"ABC\"],"
            + "[\"ABCDEFXH\",\"X\"],"
            + "[1469743199686,1469743199691],"
            + "[\"ABC\",\"Q1W2E3R4T5Y6U7I8O9P0\"],"
            + "[\"X\",\"ABCDEFXH\"],"
            + "[\"RU,IT\",\"\"],"
            + "[13621,12862],"
            + "[\"76537\",\"76537\"],"
            + "[\"12345\",\"12345\"],"
            + "[\"AUTOMATED_EXECUTION_ORDER_PRIVATE_NO_BROKER_INTERVENTION\",\"\"],"
            + "[\"\",\"683895170272\"],"
            + "[10,10],"
            + "[\"LIMIT\",\"LIMIT\"],"
            + "[\"\",\"0\"],"
            + "[473100.0,473100.0],"
            + "[\"SELL\",\"SELL\"],"
            + "[\"NQ\",\"NQ\"],"
            + "[\"DAY\",\"DAY\"],"
            + "[1469743199686,1469743199691],"
            + "[\"IJK123\",\"IJK123\"],"
            + "[\"FUTURE\",\"FUTURE\"],"
            + "[\"CRUTOMER\",\"\"],"
            + "[true,true],"
            + "[false,false],"
            + "[\"\",\"\"],"
            + "[\"\",\"\"],"
            + "[\"NFY_9\",\"\"],"
            + "[\"12345\",\"12345\"],"
            + "\"\",[31,55],"
            + "[\"\",\"RU,IT\"],"
            + "[\"NaN\",0.0],"
            + "[-2147483648,0],"
            + "[\"\",\"68250:27217624\"],"
            + "[\"\",\"NEW\"],"
            + "[\"\",\"NEW\"],"
            + "[-2147483648,2563],"
            + "[\"\",\"NEW\"],"
            + "[-2147483648,10],"
            + "[null,1469750400000],"
            + "[-2147483648,-2147483648],"
            + "[\"NaN\",\"NaN\"],"
            + "[-2147483648,-2147483648],"
            + "[null,null],"
            + "[\"\",\"\"],"
            + "[\"\",\"\"],"
            + "[\"\",\"\"],"
            + "[\"\",\"\"],"
            + "\"\",[-2147483648,-2147483648],"
            + "[\"\",\"\"],"
            + "[\"\",\"\"],"
            + "[-2147483648,-2147483648],"
            + "[\"\",\"\"],"
            + "[\"\",\"\"],"
            + "[\"\",\"\"],"
            + "[\"\",\"\"],"
            + "[\"\",\"\"],"
            + "[\"\",\"\"],"
            + "[\"\",\"\"],"
            + "[\"\",\"\"],"
            + "[\"\",\"\"],"
            + "[false,false],"
            + "[-2147483648,-2147483648],"
            + "[-2147483648,-2147483648],"
            + "[-2147483648,-2147483648],"
            + "[null,null],"
            + "[-2147483648,-2147483648],"
            + "[\"\",\"\"],"
            + "[\"NaN\",\"NaN\"],"
            + "[-2147483648,-2147483648],"
            + "[\"\",\"\"],"
            + "[-2147483648,-2147483648],"
            + "[\"\",\"\"],"
            + "[\"\",\"\"]]]";

    static {

        int largest = 20_993_248;

        StringBuilder tmpSB = new StringBuilder(largest + 6);

        while (tmpSB.length() < largest)
            tmpSB.append("0123456789ABCDE\n");

        EXTREMELY_LARGE = tmpSB.toString();
    }

    @Test
    public void testConstructed() {
        doTest(CONSTRUCTED);
    }

    @Test
    public void testExtremelyLarge() {
        assumeTrue(Jvm.is64bit());
        doTest(EXTREMELY_LARGE);
    }

    @Test
    public void testMinimal() {
        doTest(MINIMAL);
    }

    @Test
    public void testRealistic() {
        doTest(REALISTIC);
    }

    private void doTest(@NotNull String... problematic) {

        String myPath = OS.getTarget() + "/writeReadText-" + Time.uniqueId();
        doTest(myPath, SingleChronicleQueueBuilder::build, problematic);
    }

    private void doTest(String myPath,
                        java.util.function.Function<SingleChronicleQueueBuilder, ChronicleQueue> build,
                        String... problematic) {

        //! Size each invocation for its actual largest input, preserving the existing
        //! four-times margin and 256 KiB floor. Small inputs need no huge-message mapping
        //! on Windows; the 21 MB case retains its capacity and all ten round trips.
        int largestInput = Arrays.stream(problematic).mapToInt(String::length).max().orElse(0);

        //! Register this fixture's directory before opening the queue, so it is cleaned
        //! even if construction, an assertion or resource closure fails. Reverse resource
        //! order closes the queue first; try-with-resources preserves the original failure
        //! and suppresses a later deletion failure instead of replacing useful evidence.
        try (TestDirectory directory = new TestDirectory(myPath)) {
            SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.single(directory.path)
                    .blockSize(Maths.nextPower2(largestInput * 4, 256 << 10));
            try (ChronicleQueue theQueue = build.apply(builder);
                 ExcerptAppender appender = theQueue.createAppender();
                 ExcerptTailer tailer = theQueue.createTailer()) {
                long expectedBudget = 256L << 10;
                for (String input : problematic)
                    while (expectedBudget < input.length() * 4L)
                        expectedBudget *= 2;
                Assert.assertEquals("input mapping budget", expectedBudget, builder.blockSize());
                StringBuilder tmpReadback = new StringBuilder();

                // If the tests don't fail, try increasing the number of iterations
                // Setting it very high may give you a JVM crash
                final int tmpNumberOfIterations = 5;

                for (int l = 0; l < tmpNumberOfIterations; l++) {
                    for (int p = 0; p < problematic.length; p++) {
                        appender.writeText(problematic[p]);
                    }
                    for (int p = 0; p < problematic.length; p++) {
                        tailer.readText(tmpReadback);
                        Assert.assertEquals("write/readText", problematic[p], tmpReadback.toString());
                    }
                }

                for (int l = 0; l < tmpNumberOfIterations; l++) {
                    for (int p = 0; p < problematic.length; p++) {
                        final String tmpText = problematic[p];
                        appender.writeDocument(writer -> writer.getValueOut().text(tmpText));

                        tailer.readDocument(reader -> reader.getValueIn().textTo(tmpReadback));
                        String actual = tmpReadback.toString();
                        Assert.assertEquals(problematic[p].length(), actual.length());
                        for (int i = 0; i < actual.length(); i += 1024)
                            Assert.assertEquals("i: " + i, problematic[p].substring(i, Math.min(actual.length(), i + 1024)), actual.substring(i, Math.min(actual.length(), i + 1024)));
                        Assert.assertEquals(problematic[p], actual);
                    }
                }
            }
        }
    }

    @Test
    public void cleansOwnedDirectoryAfterSuccess() throws Exception {
        checkDirectoryCleanup("normal");
    }

    @Test
    public void cleansOwnedDirectoryAfterConstructionFailure() throws Exception {
        checkDirectoryCleanup("construction");
    }

    @Test
    public void cleansOwnedDirectoryAfterAssertionFailure() throws Exception {
        checkDirectoryCleanup("assertion");
    }

    @Test
    public void cleansOwnedDirectoryAfterCloseFailure() throws Exception {
        checkDirectoryCleanup("close");
    }

    private void checkDirectoryCleanup(String failureMode) throws Exception {
        File owned = new File(getTmpDir(), "text-owned");
        java.nio.file.Files.createDirectories(owned.getParentFile().toPath());
        File unrelated = new File(owned.getParentFile(), "keep");
        java.nio.file.Files.write(unrelated.toPath(), new byte[]{1});
        final IllegalStateException primary = new IllegalStateException("injected " + failureMode);
        final ChronicleQueue[] realQueue = new ChronicleQueue[1];
        try {
            Throwable caught = null;
            try {
                doTest(owned.toString(), builder -> {
                    if ("construction".equals(failureMode)) {
                        Assert.assertTrue(owned.mkdir());
                        throw primary;
                    }
                    ChronicleQueue queue = realQueue[0] = builder.build();
                    return (ChronicleQueue) java.lang.reflect.Proxy.newProxyInstance(
                            getClass().getClassLoader(), new Class<?>[]{ChronicleQueue.class}, (proxy, method, args) -> {
                                try {
                                    Object result = method.invoke(queue, args);
                                    if ("close".equals(method.getName()) && "close".equals(failureMode))
                                        throw primary;
                                    if ("createTailer".equals(method.getName()) && "assertion".equals(failureMode)) {
                                        ExcerptTailer tailer = (ExcerptTailer) result;
                                        return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                                                new Class<?>[]{ExcerptTailer.class}, (tailerProxy, operation, values) -> {
                                                    try {
                                                        Object answer = operation.invoke(tailer, values);
                                                        if ("readText".equals(operation.getName()) && values != null
                                                                && values.length == 1 && values[0] instanceof StringBuilder)
                                                            ((StringBuilder) values[0]).append("-corrupt");
                                                        return answer;
                                                    } catch (java.lang.reflect.InvocationTargetException e) {
                                                        throw e.getCause();
                                                    }
                                                });
                                    }
                                    return result;
                                } catch (java.lang.reflect.InvocationTargetException e) {
                                    throw e.getCause();
                                }
                            });
                }, "small input");
            } catch (RuntimeException | AssertionError failure) {
                caught = failure;
            }
            if ("normal".equals(failureMode))
                Assert.assertNull(caught);
            else if ("assertion".equals(failureMode)) {
                Assert.assertTrue("original text assertion", caught instanceof AssertionError);
                Assert.assertTrue(caught.getMessage().contains("write/readText"));
            } else
                Assert.assertSame("original construction/close failure", primary, caught);
            if (realQueue[0] != null)
                Assert.assertTrue("queue closed before directory cleanup", realQueue[0].isClosed());
            Assert.assertFalse("owned text directory remains", owned.exists());
            Assert.assertTrue("unrelated sibling remains", unrelated.isFile());
        } finally {
            if (realQueue[0] != null)
                realQueue[0].close();
            BackgroundResourceReleaser.releasePendingResources();
            IOTools.deleteDirWithFiles(owned);
        }
    }

    @Test
    public void drainsPendingReleasesBeforeDeletion() throws Exception {
        if (FixtureProcessTestSupport.runDeferredReleaseTest(getClass(), "drainsPendingReleasesBeforeDeletion"))
            return;
        final java.util.concurrent.atomic.AtomicBoolean released = new java.util.concurrent.atomic.AtomicBoolean();
        File owned = getTmpDir();
        java.nio.file.Files.createDirectories(owned.toPath());
        File observed = new File(owned.toString()) {
            @Override public boolean exists() {
                Assert.assertTrue("pending releases must finish before deletion", released.get());
                return super.exists();
            }
        };
        BackgroundResourceReleaser.run(() -> released.set(true));
        Assert.assertFalse("control begins with a pending release", released.get());
        try {
            new TestDirectory(observed).close();
            Assert.assertFalse("owned directory deleted", owned.exists());
        } finally {
            BackgroundResourceReleaser.releasePendingResources();
        }
    }

    @Test
    public void failedDeletionIsVisibleAndSuppressedBehindPrimaryFailure() throws Exception {
        File unrelated = getTmpDir();
        java.nio.file.Files.createDirectories(unrelated.toPath());
        File undeletable = new File(unrelated, "owned-only") {
            @Override public boolean exists() { return true; }
            @Override public boolean isDirectory() { return true; }
            @Override public File[] listFiles() { return new File[0]; }
            @Override public boolean delete() { return false; }
        };
        AssertionError deletion = Assert.assertThrows(AssertionError.class,
                () -> new TestDirectory(undeletable).close());
        Assert.assertTrue(deletion.getMessage().contains("Could not delete test directory"));
        IllegalStateException primary = new IllegalStateException("original body failure");
        try {
            try (TestDirectory ignored = new TestDirectory(undeletable)) {
                throw primary;
            }
        } catch (IllegalStateException caught) {
            Assert.assertSame(primary, caught);
            Assert.assertEquals("cleanup failure is suppressed", 1, caught.getSuppressed().length);
            Assert.assertTrue(caught.getSuppressed()[0] instanceof AssertionError);
        }
        Assert.assertTrue("cleanup did not delete the unrelated parent", unrelated.isDirectory());
    }

    private static final class TestDirectory implements AutoCloseable {
        private final File path;

        private TestDirectory(String path) {
            this(new File(path));
        }

        private TestDirectory(File path) {
            this.path = path;
        }

        @Override
        public void close() {
            //! Queue close can enqueue unmapping; finish those releases before Windows deletion.
            BackgroundResourceReleaser.releasePendingResources();
            //! A false deletion result is a cleanup failure too. Limit deletion to the
            //! unique path created by this invocation, leaving other tests' files alone.
            if (path.exists() && !IOTools.deleteDirWithFiles(path))
                throw new AssertionError("Could not delete test directory " + path);
        }
    }
}
