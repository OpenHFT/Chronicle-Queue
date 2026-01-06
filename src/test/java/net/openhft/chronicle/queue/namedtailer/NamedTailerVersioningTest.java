/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.jupiter.api.Assertions.*;

public class NamedTailerVersioningTest extends QueueTestCommon {

    @Test
    @DisplayName("Non-replicated named tailer leaves version unset")
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
            assertEquals(Long.MIN_VALUE, longValue.getValue(), "non-replicated named tailer: version unset");
            longValue.close();

            finishedNormally = true;
        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    @Test
    @DisplayName("Backwards compatibility retains stored tailer positions")
    public void verifyBackwardsCompatibility_tailerPositionsAreRetained() throws IOException, URISyntaxException {
        Assumptions.assumeFalse(PageUtil.isHugePage(OS.getTarget()), "This test must be ignored on hugetlbfs because the test file was generated on a standard linux file system");

        // Copy the data from src/test/resources
        Path templatePath = Paths.get(this.getClass().getResource("/named-tailer/5.25ea1-backwards-compat").toURI());
        Path targetPath = Paths.get(OS.getTarget()).resolve(templatePath.getFileName());
        copyFolder(templatePath, targetPath);

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(targetPath).build();
             ExcerptTailer tailerOne = queue.createTailer("replicated:tailerOne");
             ExcerptTailer tailerTwo = queue.createTailer("replicated:tailerTwo");
             ExcerptTailer tailerThree = queue.createTailer("replicated:tailerThree")) {

            assertEquals(84512071483394L, tailerOne.index(), "backwards compat: tailerOne index");
            assertEquals(84512071483394L, tailerTwo.index(), "backwards compat: tailerTwo index");
            assertEquals(84512071483394L, tailerThree.index(), "backwards compat: tailerThree index");

            try (LongValue tailerOneVersion = queue.indexVersionForId("replicated:tailerOne");
                 LongValue tailerTwoVersion = queue.indexVersionForId("replicated:tailerTwo");
                 LongValue tailerThreeVersion = queue.indexVersionForId("replicated:tailerThree")) {
                assertEquals(0, tailerOneVersion.getValue(), "backwards compat: tailerOne version");
                assertEquals(0, tailerTwoVersion.getValue(), "backwards compat: tailerTwo version");
                assertEquals(0, tailerThreeVersion.getValue(), "backwards compat: tailerThree version");
            }

        } finally {
            IOTools.deleteDirWithFiles(targetPath.toString());
        }
    }

    @Test
    @DisplayName("Retain version and index across lifecycles")
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
            assertNotEquals(0, index, "tailer index should advance after initial read");
        }

        // Open for the second time ensure that the tailer position was retained
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build();
             ExcerptTailer tailer = queue.createTailer("replicated:named_1")) {
            assertEquals(index, tailer.index(), "tailer restart: retained index");
        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    @Test
    @DisplayName("No version increments without replication activity")
    public void noVersionIncrements() {
        File queuePath = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer("replicated:named_1")) {
            assertNotNull(appender, "no version increments: appender created");
            assertNotNull(tailer, "no version increments: tailer created");

            LongValue indexVersion = queue.indexVersionForId("replicated:named_1");
            assertEquals(-1, indexVersion.getValue(), "no version increments: initial version");
            indexVersion.close();

        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    @Test
    @DisplayName("Version increments with repeated reads of data")
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
            assertEquals(100, indexVersion.getValue(), "multiple version increments: version");
            indexVersion.close();

        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    @Test
    @DisplayName("Named tailer can rewind to start")
    public void namedTailerCanRewindToStart() {
        File queuePath = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer("replicated:rewind")) {

            for (int i = 0; i < 3; i++) {
                appender.writeText("msg-" + i);
            }
            assertEquals("msg-0", tailer.readText(), "rewind should read first message");
            assertEquals("msg-1", tailer.readText(), "rewind should read second message");

            tailer.toStart();
            assertEquals("msg-0", tailer.readText(), "rewind should return to first message after toStart");
        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    @Test
    @DisplayName("Named tailer resumes stored index after restart")
    public void namedTailerCanMoveToStoredIndexAfterRestart() {
        File queuePath = getTmpDir();
        long[] indexes = new long[4];
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build();
             ExcerptAppender appender = queue.createAppender()) {
            for (int i = 0; i < indexes.length; i++) {
                appender.writeText("payload-" + i);
                indexes[i] = appender.lastIndexAppended();
            }
        }

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build();
             ExcerptTailer tailer = queue.createTailer("replicated:resumer")) {
            assertTrue(tailer.moveToIndex(indexes[2]), "tailer should move to stored index 2");
            assertEquals("payload-2", tailer.readText(), "moveToIndex should read payload-2");
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
