/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.namedtailer;

import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.Assert.*;

public class NamedTailerVersioningTest extends QueueTestCommon {

    @Test
    public void nonReplicatedNamedTailerShouldNotCreateVersionInMetdata() {
        finishedNormally = false;
        File queuePath = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer("named_1")) {

            appender.writeText("Test");
            appender.writeText("Test");
            appender.writeText("Test");
            tailer.readText();
            tailer.readText();
            tailer.readText();

            LongValue longValue = queue.metaStore().acquireValueFor(String.format(SingleChronicleQueue.INDEX_VERSION_FORMAT, "named_1"));
            assertEquals(Long.MIN_VALUE, longValue.getValue());
            longValue.close();

            finishedNormally = true;
        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    @Test
    public void verifyBackwardsCompatibility_tailerPositionsAreRetained() throws IOException, URISyntaxException {
        Assume.assumeFalse("This test must be ignored on hugetlbfs because the test file was generated on a standard linux file system", PageUtil.isHugePage(OS.getTarget()));

        // Copy the data from src/test/resources
        Path templatePath = Paths.get(this.getClass().getResource("/named-tailer/5.25ea1-backwards-compat").toURI());
        Path targetPath = Paths.get(OS.getTarget()).resolve(templatePath.getFileName());
        copyFolder(templatePath, targetPath);

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(targetPath).build();
             ExcerptTailer tailerOne = queue.createTailer("replicated:tailerOne");
             ExcerptTailer tailerTwo = queue.createTailer("replicated:tailerTwo");
             ExcerptTailer tailerThree = queue.createTailer("replicated:tailerThree")) {

            assertEquals(84512071483394L, tailerOne.index());
            assertEquals(84512071483394L, tailerTwo.index());
            assertEquals(84512071483394L, tailerThree.index());

            try (LongValue tailerOneVersion = queue.indexVersionForId("replicated:tailerOne");
                 LongValue tailerTwoVersion = queue.indexVersionForId("replicated:tailerTwo");
                 LongValue tailerThreeVersion = queue.indexVersionForId("replicated:tailerThree")) {
                assertEquals(0, tailerOneVersion.getValue());
                assertEquals(0, tailerTwoVersion.getValue());
                assertEquals(0, tailerThreeVersion.getValue());
            }

        } finally {
            IOTools.deleteDirWithFiles(targetPath.toString());
        }
    }

    @Test
    public void versionAndIndexRetentionAcrossMultipleLifecycles() {
        File queuePath = getTmpDir();

        // Open for first time
        long index;
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer("replicated:named_1")) {

            appender.writeText("hello");
            tailer.readText();
            index = tailer.index();
            assertNotEquals(0, index);
        }

        // Open for the second time ensure that the tailer position was retained
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build();
             ExcerptTailer tailer = queue.createTailer("replicated:named_1")) {
            assertEquals(index, tailer.index());
        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    @Test
    public void noVersionIncrements() {
        File queuePath = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer("replicated:named_1")) {
            assertNotNull(appender);
            assertNotNull(tailer);

            LongValue indexVersion = queue.indexVersionForId("replicated:named_1");
            assertEquals(-1, indexVersion.getValue());
            indexVersion.close();

        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    @Test
    public void multipleVersionIncrements() {
        File queuePath = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer("replicated:named_1")) {

            int versions = 100;
            for (int i = 0; i < versions; i++) {
                appender.writeText("test");
                tailer.readText();
            }

            LongValue indexVersion = queue.indexVersionForId("replicated:named_1");
            assertEquals(100, indexVersion.getValue());
            indexVersion.close();

        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    private void copyFolder(Path src, Path dest) throws IOException {
        try (Stream<Path> stream = Files.walk(src)) {
            stream.forEach(source -> copy(source, dest.resolve(src.relativize(source))));
        }
    }

    private void copy(Path source, Path dest) {
        try {
            Files.copy(source, dest, REPLACE_EXISTING);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
