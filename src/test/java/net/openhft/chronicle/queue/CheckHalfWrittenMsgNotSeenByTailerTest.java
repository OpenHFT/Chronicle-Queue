package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class CheckHalfWrittenMsgNotSeenByTailerTest {
    static {
        // load the lass
        HalfWriteAMessage.class.getName();
    }

    public static class HalfWriteAMessage {

        // writes three messages the third messas is half written
        public static void main(String[] args) throws InterruptedException {
            System.out.println("half writing a message to " + args[0]);


            try (final ChronicleQueue single = ChronicleQueue.single(args[0]);
                 final ExcerptAppender excerptAppender = single.acquireAppender()) {

                try (final DocumentContext dc = excerptAppender.writingDocument()) {
                    dc.wire().write("key1").text("hello world 1");
                    dc.wire().write("key2").text("hello world 2");
                }

                try (final DocumentContext dc = excerptAppender.writingDocument()) {
                    dc.wire().write("key1").text("hello world 3");
                    dc.wire().write("key2").text("hello world 4");
                }

                try (final DocumentContext dc = excerptAppender.writingDocument()) {
                    dc.wire().write("key1").text("hello world 5");

                    // give time to flush
                    Thread.sleep(1);

                    System.out.println("== FINISHED WRITING DATA ==");

                    // this will create a half written message, as we are going to system exit
                    System.exit(-1);

                    dc.wire().write("key2").text("hello world 6");
                }
            }
        }
    }


    @Test
    public void checkTailerOnlyReadsTwoMessage() throws IOException, InterruptedException {
        Assume.assumeTrue(!OS.isWindows());
        final File queueDirectory = DirectoryUtils.tempDir("halfWritten");

        final String command = String.format("mvn compile exec:java -Dexec.classpathScope=test " +
                "-Dexec.mainClass=%s -Dexec.args=\"%s\"", HalfWriteAMessage.class.getName(), queueDirectory.getAbsoluteFile());
        runCommand(command);

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
                Assert.assertFalse(present);
            }
        }
    }

    private static void runCommand(String command) throws IOException, InterruptedException {
        Process p = Runtime.getRuntime().exec(command);
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
