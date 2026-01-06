/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.scoped.ScopedResource;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.WireType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.HOURLY;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@SuppressWarnings({"deprecation", "removal"})
public class SingleChronicleQueueBuilderTest extends QueueTestCommon {
    private static final String TEST_QUEUE_FILE = "src/test/resources/tr2/20170320.cq4";
    private static final String BASE_PATH = OS.getTarget() + "/singleChronicleQueueBuilderTest";

    @AfterAll
    public static void afterClass() {
        IOTools.deleteDirWithFiles(BASE_PATH, 2);
    }

    @Test
    @DisplayName("Queue file path resolves to directory")
    public void shouldDetermineQueueDirectoryFromQueueFile() throws IOException {
        ignoreException("reading control code as text");
        ignoreException("Unable to copy TimedStoreRecovery safely");
        expectException("Queues should be configured with the queue directory, not a specific filename");
        ignoreException("Metadata file not found in readOnly mode");
        expectException("Unexpected field lastAcknowledgedIndexReplicated");

        final Path path = Paths.get(TEST_QUEUE_FILE);
        final Path metadata = Paths.get(path.getParent().toString(), "metadata.cq4t");
        if (metadata.toFile().exists())
            Files.delete(metadata);

        try (final ChronicleQueue queue =
                     ChronicleQueue.singleBuilder(path)
                             .testBlockSize()
                             .readOnly(true)
                             .build();
             final ExcerptTailer tailer = queue.createTailer();
             final DocumentContext dc = tailer.readingDocument()) {
            assertFalse(dc.isPresent(), "queue file path: no document present");

        } finally {
            IOTools.deleteDirWithFiles(path.toFile(), 20);
        }
        assertTrue(new File(TEST_QUEUE_FILE).length() < (1 << 20),
                "queue file path should keep file size under 1MiB");
    }

    @Test
    @DisplayName("Queue file with wrong extension is rejected")
    public void shouldThrowExceptionIfQueuePathIsFileWithIncorrectExtension() throws IOException {
        final File tempFile = File.createTempFile(SingleChronicleQueueBuilderTest.class.getSimpleName(), ".txt");
        tempFile.deleteOnExit();
        assertThrows(IllegalArgumentException.class,
                () -> SingleChronicleQueueBuilder.binary(tempFile),
                "queue path: incorrect extension");
    }

    @Test
    @DisplayName("Set all null fields copies builder values")
    public void setAllNullFields() {
        SingleChronicleQueueBuilder b1 = SingleChronicleQueueBuilder.builder();
        SingleChronicleQueueBuilder b2 = SingleChronicleQueueBuilder.builder();
        b1.blockSize(1234567);
        b2.bufferCapacity(98765);
        b2.setAllNullFields(b1);
        assertEquals(1234567, b2.blockSize(), "Block size should copy from source builder");
        assertEquals(98765, b2.bufferCapacity(), "Buffer capacity should copy from source builder");
    }

    @Test
    @DisplayName("Set all null fields rejects different hierarchy")
    public void setAllNullFieldsShouldFailWithDifferentHierarchy() {
        OneExtendedBuilder b1 = new OneExtendedBuilder();
        OtherExtendedBuilder b2 = new OtherExtendedBuilder();
        b2.bufferCapacity(98765);
        b1.blockSize(1234567);
        assertThrows(IllegalArgumentException.class,
                () -> b2.setAllNullFields(b1),
                "setAllNullFields should reject builders with different hierarchy");
    }

    static class OneExtendedBuilder extends SingleChronicleQueueBuilder {
    }

    static class OtherExtendedBuilder extends SingleChronicleQueueBuilder {
    }

    @Test
    @DisplayName("Marshallable builder round trip preserves epoch")
    public void testReadMarshallable() {
        expectException("Overriding roll epoch from existing metadata");
        final String tmpDir = getTmpDir().toString();
        SingleChronicleQueueBuilder builder = Marshallable.fromString("!net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder {\n" +
                "  writeBufferMode: None,\n" +
                "  readBufferMode: None,\n" +
                "  wireType: BINARY_LIGHT,\n" +
                "  path: " + tmpDir + ",\n" +
                "  rollCycle: !net.openhft.chronicle.queue.rollcycles.LegacyRollCycles DAILY,\n" +
                "  timeProvider: !net.openhft.chronicle.core.time.SystemTimeProvider INSTANCE,\n" +
                "  rollTime: 17:02,\n" +
                "  rollTimeZone: !java.time.ZoneRegion {\n" +
                "    id: UTC\n" +
                "  }," +
                "}\n");
        builder.build().close();
        assertEquals(61320000, builder.epoch(), "Epoch should be read from marshallable text");

        SingleChronicleQueueBuilder builder2 = Marshallable.fromString(builder.toString());
        builder2.build().close();
        assertEquals(61320000, builder2.epoch(), "Epoch should remain after marshallable round trip");
    }

    @Test
    @DisplayName("Marshallable builder round trip via binary wire")
    public void testWriteMarshallableBinary() {
        final SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.single(BASE_PATH).rollCycle(HOURLY);

        builder.build().close();
        try (final ScopedResource<Wire> wireTl = Wires.acquireBinaryWireScoped()) {
            Wire wire = wireTl.get();
            wire.usePadding(true);
            wire.write().typedMarshallable(builder);

            SingleChronicleQueueBuilder builder2 = wire.read().typedMarshallable();
            assertEquals(builder, builder2, "Builder should round trip via binary wire");
            builder2.build().close();
        }
    }

    @Test
    @DisplayName("Marshallable builder round trip via text wire")
    public void testWriteMarshallable() {
        final SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.single(BASE_PATH).rollCycle(HOURLY);

        builder.build().close();
        String val = Marshallable.$toString(builder);

        SingleChronicleQueueBuilder builder2 = Marshallable.fromString(val);
        assertEquals(builder, builder2, "Builder should round trip via text marshallable");
        builder2.build().close();
    }

    @Test
    @DisplayName("Override sourceId reads existing queue metadata")
    public void tryOverrideSourceId() {
        expectException("Overriding sourceId from existing metadata");

        final File tmpDir = getTmpDir();
        final int firstSourceId = 1;
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(tmpDir).sourceId(firstSourceId).build()) {
            assertNotNull(queue, "Queue should be created with initial sourceId");
            // just create the queue
        }
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(tmpDir).sourceId(firstSourceId + 1).build()) {
            assertEquals(firstSourceId, q.sourceId(), "SourceId should be read from existing metadata");
        }
    }

    @Test
    @DisplayName("Read-only builder ignores createAppender condition")
    public void buildWillNotSetCreateAppenderConditionWhenQueueIsReadOnly() {
        assumeFalse(OS.isWindows(), "read-only rebuild requires non-Windows file handling");

        final File tmpDir = getTmpDir();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(tmpDir).build()) {
            assertNotNull(queue, "Queue should be created before read-only rebuild");
            // just create the queue
        }

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(tmpDir)
                .createAppenderConditionCreator(q -> {
                    fail("This should never be called");
                    return null;
                })
                .readOnly(true)
                .build()) {
            assertNotNull(queue, "Read-only queue should ignore createAppender condition");
            // This will throw if we attempt to create the createAppender condition
        }
    }

    /**
     * Ensure that drainer priority is set to default value on constructing a SingleChronicleQueueBuilder
     */
    @Test
    @DisplayName("Drainer priority defaults to configured value")
    public void drainerPriorityIsSetByDefault() {
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.single();
        assertNotNull(builder.drainerPriority(), "Drainer priority should be set by default"); // priority may change from CONCURRENT in future
    }

    @Test
    @DisplayName("Builder applies core overrides for logger configs")
    public void builderAppliesCoreOverridesForLoggerConfigs() {
        final File tmpDir = getTmpDir();
        final int blockSize = 512 << 10;
        final long requestedBufferCapacity = 64 << 10;
        final RollCycle rollCycle = TestRollCycles.TEST_SECONDLY;
        final int sourceId = 7;
        final WireType wireType = WireType.BINARY_LIGHT;
        final List<String> messages = Arrays.asList("first log entry", "second log entry");

        final SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.single(tmpDir)
                .blockSize(blockSize)
                .bufferCapacity(requestedBufferCapacity)
                .rollCycle(rollCycle)
                .sourceId(sourceId)
                .wireType(wireType);
        final long expectedBlockSize = builder.blockSize();
        final long expectedBufferCapacity = builder.bufferCapacity();

        try (SingleChronicleQueue queue = builder.build()) {
            verifyQueueOverrides(queue, expectedBlockSize, expectedBufferCapacity, rollCycle, wireType, sourceId);

            try (ExcerptAppender appender = queue.acquireAppender()) {
                messages.forEach(appender::writeText);
            }

            try (ExcerptTailer tailer = queue.createTailer()) {
                for (String expected : messages) {
                    assertEquals(expected, tailer.readText(),
                            "logger config should read message " + expected);
                }
            }
        }

        try (SingleChronicleQueue reopened = SingleChronicleQueueBuilder.single(tmpDir)
                .wireType(wireType)
                .sourceId(sourceId)
                .rollCycle(rollCycle)
                .build();
             ExcerptTailer tailer = reopened.createTailer()) {
            assertEquals(rollCycle, reopened.rollCycle(), "Roll cycle should be read from metadata");
            assertEquals(sourceId, reopened.sourceId(), "SourceId should be read from metadata");

            for (String expected : messages) {
                assertEquals(expected, tailer.readText(),
                        "logger config should read message after reopen: " + expected);
            }
        } finally {
            IOTools.deleteDirWithFiles(tmpDir);
        }
    }

    private static void verifyQueueOverrides(SingleChronicleQueue queue,
                                             long expectedBlockSize,
                                             long expectedBufferCapacity,
                                             RollCycle expectedRollCycle,
                                             WireType expectedWireType,
                                             int expectedSourceId) {
        assertEquals(expectedBlockSize, queue.blockSize(), "queue override should preserve blockSize");
        assertEquals(expectedBufferCapacity, queue.bufferCapacity(), "queue override should preserve bufferCapacity");
        assertEquals(expectedRollCycle, queue.rollCycle(), "queue override should preserve rollCycle");
        assertEquals(expectedWireType, queue.wireType(), "queue override should preserve wireType");
        assertEquals(expectedSourceId, queue.sourceId(), "queue override should preserve sourceId");
    }
}
