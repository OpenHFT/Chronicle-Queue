package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.testframework.process.ProcessRunner;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public class CheckHalfWrittenMsgNotSeenByTailerTest extends QueueTestCommon {
    static {
        // load the lass
        HalfWriteAMessage.class.getName();
    }

    public static class HalfWriteAMessage {

        // writes three messages the third messas is half written
        public static void main(String[] args) throws InterruptedException {
            writeIncompleteMessage(args[0], true);
        }

        private static void writeIncompleteMessage(String arg, boolean exit) throws InterruptedException {
            System.out.println("half writing a message to " + arg);

            try( final ChronicleQueue single = ChronicleQueue.single(arg) ) {
                final ExcerptAppender excerptAppender = single.acquireAppender();

                try (final DocumentContext dc = excerptAppender.writingDocument()) {
                    dc.wire().write("key1").text("hello world 1");
                    dc.wire().write("key2").text("hello world 2");
                }

                try (final DocumentContext dc = excerptAppender.writingDocument()) {
                    dc.wire().write("key1").text("hello world 3");
                    dc.wire().write("key2").text("hello world 4");
                }

                DocumentContext dc = excerptAppender.writingDocument();
                dc.wire().write("key1").text("hello world 5");

                // give time to flush
                Thread.sleep(1);

                System.out.println("== FINISHED WRITING DATA ==");

                // this will create a half written message, as we are going to system exit
                if (exit)
                    System.exit(-1);

                dc.wire().write("key2").text("hello world 6");
            }
        }
    }

    @Test
    public void checkTailerOnlyReadsTwoMessageOneProcess() throws InterruptedException {
        Assume.assumeTrue(!OS.isWindows());
        final File queueDirectory = DirectoryUtils.tempDir("halfWritten");

        HalfWriteAMessage.writeIncompleteMessage(queueDirectory.toString(), false);
        for (int i = 0; i < 3; i++) {
            System.gc();
            Jvm.pause(50);
        }

        try (final ChronicleQueue single = ChronicleQueue.single(queueDirectory.getPath());
             final ExcerptTailer tailer = single.createTailer()) {

            try (final DocumentContext dc = tailer.readingDocument()) {
                Assert.assertTrue(dc.isPresent());
                Assert.assertEquals("hello world 1", dc.wire().read("key1").text());
                Assert.assertEquals("hello world 2", dc.wire().read("key2").text());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                Assert.assertTrue(dc.isPresent());
                Assert.assertEquals("hello world 3", dc.wire().read("key1").text());
                Assert.assertEquals("hello world 4", dc.wire().read("key2").text());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                final boolean present = dc.isPresent();
                if (present) {
                    System.out.println(dc.wire().bytes().toHexString());
                    String key = dc.wire().readEvent(String.class);
                    String value = dc.wire().getValueIn().text();
                    fail("key: " + key + ", value: " + value);
                }
            }
        }
    }

    @Test
    public void checkTailerOnlyReadsTwoMessageTwoProcesses() throws IOException, InterruptedException {
        Assume.assumeTrue(!OS.isWindows());
        Assume.assumeTrue(!OS.isMacOSX());
        final File queueDirectory = DirectoryUtils.tempDir("halfWritten");

        runCommand(ProcessRunner.runClass(HalfWriteAMessage.class, queueDirectory.getAbsolutePath()));

        try (final ChronicleQueue single = ChronicleQueue.single(queueDirectory.getPath());
             final ExcerptTailer tailer = single.createTailer()) {

            try (final DocumentContext dc = tailer.readingDocument()) {
                Assert.assertTrue(dc.isPresent());
                Assert.assertEquals("hello world 1", dc.wire().read("key1").text());
                Assert.assertEquals("hello world 2", dc.wire().read("key2").text());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                Assert.assertTrue(dc.isPresent());
                Assert.assertEquals("hello world 3", dc.wire().read("key1").text());
                Assert.assertEquals("hello world 4", dc.wire().read("key2").text());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                final boolean present = dc.isPresent();
                if (present) {
                    Jvm.error().on(getClass(), "Found an excerpt " + dc.wire().bytes().toHexString());

                    String key = dc.wire().readEvent(String.class);
                    String value = dc.wire().getValueIn().text();
                    fail("key: " + key + ", value: " + value);
                }
            }
        }
    }

    private static void runCommand(Process p) throws IOException, InterruptedException {
        BufferedReader stdInput = new BufferedReader(new
                InputStreamReader(p.getInputStream()));

        BufferedReader stdError = new BufferedReader(new
                InputStreamReader(p.getErrorStream()));

        String s;
        // read the output from the command
        //      System.out.println("Here is the standard output of the command:\n");
        while ((s = stdInput.readLine()) != null) {

            System.out.println(s);

            // wait for Replication Started
            if ("== FINISHED WRITING DATA ==".equals(s))
                return;

        }

        // read any errors from the attempted command
        System.out.println("Here is the standard error of the command (if any):\n");
        while ((s = stdError.readLine()) != null) {
            System.out.println(s);
        }
        p.waitFor();
    }
}
