/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.testframework.process.JavaProcessBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.junit.jupiter.api.Assertions.fail;

public class CheckHalfWrittenMsgNotSeenByTailerTest extends QueueTestCommon {
    public static class HalfWriteAMessage {

        // writes three messages the third messas is half written
        public static void main(String[] args) throws InterruptedException {
            writeIncompleteMessage(args[0], true);
        }

        private static void writeIncompleteMessage(String arg, boolean exit) throws InterruptedException {
            System.out.println("half writing a message to " + arg);

            try (final ChronicleQueue single = ChronicleQueue.single(arg);
                 final ExcerptAppender excerptAppender = single.createAppender()) {

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
    @DisplayName("Tailer reads two complete messages in a single process")
    public void checkTailerOnlyReadsTwoMessageOneProcess() throws InterruptedException {
        Assumptions.assumeTrue(!OS.isWindows(), "Test requires non-Windows file semantics");
        final File queueDirectory = DirectoryUtils.tempDir("halfWritten");

        HalfWriteAMessage.writeIncompleteMessage(queueDirectory.toString(), false);
        for (int i = 0; i < 3; i++) {
            System.gc();
            Jvm.pause(50);
        }

        try (final ChronicleQueue single = ChronicleQueue.single(queueDirectory.getPath());
             final ExcerptTailer tailer = single.createTailer()) {

            assertHalfWrittenReads(tailer);
        }
    }

    @Test
    @DisplayName("Tailer reads two complete messages across two processes")
    public void checkTailerOnlyReadsTwoMessageTwoProcesses() throws IOException, InterruptedException {
        Assumptions.assumeTrue(OS.isLinux() && OS.is64Bit() && !isWsl(), "Test requires Linux 64-bit process behaviour");
        ignoreException("Forced unlocking `chronicle.write.lock` in lock file:target/halfWritten");

        final File queueDirectory = DirectoryUtils.tempDir("halfWritten");

        runCommand(JavaProcessBuilder.create(HalfWriteAMessage.class).withProgramArguments(queueDirectory.getAbsolutePath()).start());
        // as exit is true, we could get a forced unlock exception
        ignoreException("Forced unlocking");

        try (final ChronicleQueue single = ChronicleQueue.single(queueDirectory.getPath());
             final ExcerptTailer tailer = single.createTailer()) {

            assertHalfWrittenReads(tailer);
        }
    }

    private void assertHalfWrittenReads(ExcerptTailer tailer) {
        try (final DocumentContext dc = tailer.readingDocument()) {
            Assertions.assertTrue(dc.isPresent(), "first document context should be present in tailer");
            Assertions.assertEquals("hello world 1", dc.wire().read("key1").text(),
                    "first document key1 should be 'hello world 1'");
            Assertions.assertEquals("hello world 2", dc.wire().read("key2").text(),
                    "first document key2 should be 'hello world 2'");
        }

        try (final DocumentContext dc = tailer.readingDocument()) {
            Assertions.assertTrue(dc.isPresent(), "second document context should be present in tailer");
            Assertions.assertEquals("hello world 3", dc.wire().read("key1").text(),
                    "second document key1 should be 'hello world 3'");
            Assertions.assertEquals("hello world 4", dc.wire().read("key2").text(),
                    "second document key2 should be 'hello world 4'");
        }

        try (final DocumentContext dc = tailer.readingDocument()) {
            final boolean present = dc.isPresent();
            if (present) {
                String key = dc.wire().readEvent(String.class);
                String value = dc.wire().getValueIn().text();
                fail("key: " + key + ", value: " + value);
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
