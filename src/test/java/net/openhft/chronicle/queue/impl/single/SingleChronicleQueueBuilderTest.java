package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.Wires;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static net.openhft.chronicle.queue.RollCycles.HOURLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SingleChronicleQueueBuilderTest extends ChronicleQueueTestBase {
    private static final String TEST_QUEUE_FILE = "src/test/resources/tr2/20170320.cq4";

    @Test
    public void shouldDetermineQueueDirectoryFromQueueFile() throws IOException {
        expectException("reading control code as text");

        final Path path = Paths.get(OS.USER_DIR, TEST_QUEUE_FILE);
        final Path metadata = Paths.get(path.getParent().toString(), "metadata.cq4t");
        if (metadata.toFile().exists())
            Files.delete(metadata);
        try (final ChronicleQueue queue =
                     ChronicleQueue.singleBuilder(path)
                             .testBlockSize()
                             .build()) {
            assertFalse(queue.createTailer().readingDocument().isPresent());
        } finally {
            IOTools.deleteDirWithFiles(path.toFile(), 20);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfQueuePathIsFileWithIncorrectExtension() throws IOException {
        final File tempFile = File.createTempFile(SingleChronicleQueueBuilderTest.class.getSimpleName(), ".txt");
        tempFile.deleteOnExit();
        SingleChronicleQueueBuilder.
                binary(tempFile);
    }

    @Test
    public void setAllNullFields() {
        SingleChronicleQueueBuilder b1 = SingleChronicleQueueBuilder.builder();
        SingleChronicleQueueBuilder b2 = SingleChronicleQueueBuilder.builder();
        b1.blockSize(1234567);
        b2.bufferCapacity(98765);
        b2.setAllNullFields(b1);
        assertEquals(1234567, b2.blockSize());
        assertEquals(98765, b2.bufferCapacity());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setAllNullFieldsShouldFailWithDifferentHierarchy() {
        SingleChronicleQueueBuilder b1 = Wires.tupleFor(SingleChronicleQueueBuilder.class, "ChronicleQueueBuilder");
        SingleChronicleQueueBuilder b2 = SingleChronicleQueueBuilder.builder();
        b2.bufferCapacity(98765);
        b1.blockSize(1234567);
        b2.setAllNullFields(b1);
    }

    @Test
    public void testReadMarshallable() {
        expectException("Overriding roll epoch from existing metadata");
        SingleChronicleQueueBuilder builder = Marshallable.fromString("!net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder {\n" +
                "  writeBufferMode: None,\n" +
                "  readBufferMode: None,\n" +
                "  wireType: BINARY_LIGHT,\n" +
                "  path: " + getTmpDir() +
                "  rollCycle: !net.openhft.chronicle.queue.RollCycles DAILY,\n" +
                "  timeProvider: !net.openhft.chronicle.core.time.SystemTimeProvider INSTANCE,\n" +
                "  rollTime: 17:02,\n" +
                "  rollTimeZone: !java.time.ZoneRegion {\n" +
                "    id: UTC\n" +
                "  }," +
                "}\n");
        builder.build().close();
        assertEquals(61320000, builder.epoch());

        SingleChronicleQueueBuilder builder2 = Marshallable.fromString(builder.toString());
        builder2.build().close();
        assertEquals(61320000, builder2.epoch());
    }

    @Test
    @Ignore("https://github.com/OpenHFT/Chronicle-Wire/issues/165")
    public void testWriteMarshallableBinary() {
        final SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.single("test").rollCycle(HOURLY);

        builder.build().close();
        final Wire wire = Wires.acquireBinaryWire();
        wire.write().typedMarshallable(builder);

        System.err.println(wire.bytes().toHexString());

        SingleChronicleQueueBuilder builder2 = wire.read().typedMarshallable();
        builder2.build().close();
    }

    @Test
    public void testWriteMarshallable() {
        final SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.single("test").rollCycle(HOURLY);

        builder.build().close();
        String val = Marshallable.$toString(builder);

        System.err.println(val);

        SingleChronicleQueueBuilder builder2 = Marshallable.fromString(val);
        builder2.build().close();
    }

    @Test
    public void tryOverrideSourceId() {
        expectException("Overriding sourceId from existing metadata");

        final File tmpDir = getTmpDir();
        final int firstSourceId = 1;
        try (ChronicleQueue ignored = SingleChronicleQueueBuilder.single(tmpDir).sourceId(firstSourceId).build()) {
            // just create the queue
        }
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(tmpDir).sourceId(firstSourceId + 1).build()) {
            assertEquals(firstSourceId, q.sourceId());
        }
    }
}