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

        //! Size each invocation for its actual largest input, preserving the existing
        //! four-times margin and 256 KiB floor. Small inputs need no huge-message mapping
        //! on Windows; the 21 MB case retains its capacity and all ten round trips.
        int largestInput = Arrays.stream(problematic).mapToInt(String::length).max().orElse(0);

        //! Register this fixture's directory before opening the queue, so it is cleaned
        //! even if construction, an assertion or resource closure fails. Reverse resource
        //! order closes the queue first; try-with-resources preserves the original failure
        //! and suppresses a later deletion failure instead of replacing useful evidence.
        try (TestDirectory directory = new TestDirectory(myPath);
             ChronicleQueue theQueue = SingleChronicleQueueBuilder
                .single(directory.path)
                .blockSize(Maths.nextPower2(largestInput * 4, 256 << 10))
                .build();
             ExcerptAppender appender = theQueue.createAppender()) {

            ExcerptTailer tailer = theQueue.createTailer();

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

    private static final class TestDirectory implements AutoCloseable {
        private final File path;

        private TestDirectory(String path) {
            this.path = new File(path);
        }

        @Override
        public void close() {
            //! Drain queued background releases first, so mapped files no longer pin the
            //! directory when it is deleted.
            BackgroundResourceReleaser.releasePendingResources();
            //! A false deletion result is a cleanup failure too. Limit deletion to the
            //! unique path created by this invocation, leaving other tests' files alone.
            if (path.exists() && !IOTools.deleteDirWithFiles(path))
                throw new AssertionError("Could not delete test directory " + path);
        }
    }
}
