package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueExcerpts.InternalAppender;
import net.openhft.chronicle.wire.Wires;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static net.openhft.chronicle.bytes.Bytes.elasticByteBuffer;
import static net.openhft.chronicle.bytes.Bytes.fromString;

public class ChronicleQueueIndexTest {

    @Test
    public void checkTheEOFisWrittenToPreQueueFile() {

        SetTimeProvider tp = new SetTimeProvider(System.nanoTime());
        File firstCQFile = null;

        File file1 = DirectoryUtils.tempDir("indexQueueTest2");
        try {
            try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                    .path(file1)
                    .rollCycle(RollCycles.DAILY)
                    .timeProvider(tp)
                    .build()) {
                InternalAppender appender = (InternalAppender) queue.acquireAppender();

                appender.writeBytes(RollCycles.DAILY.toIndex(1, 0L), fromString("Hello World 1"));

                // Simulate the end of the day i.e the queue closes the day rolls
                // (note the change of index from 18264 to 18265)
                firstCQFile = queue.file();
                firstCQFile = requireNonNull(firstCQFile.listFiles((dir, name) -> name.endsWith(".cq4")))[0];
                Assert.assertFalse(hasEOFAtEndOfFile(firstCQFile));
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail();
            }

            tp.advanceMillis(TimeUnit.DAYS.toMillis(2));

            try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                    .path(file1)
                    .rollCycle(RollCycles.DAILY)
                    .timeProvider(tp)
                    .build();) {
                InternalAppender appender = (InternalAppender) queue.acquireAppender();

                appender.writeBytes(RollCycles.DAILY.toIndex(3, 0L), fromString("Hello World 2"));

                // Simulate the end of the day i.e the queue closes the day rolls
                // (note the change of index from 18264 to 18265)

                Assert.assertTrue(hasEOFAtEndOfFile(firstCQFile));

            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail();
            }
        } finally {
            file1.deleteOnExit();
        }

    }

    private boolean hasEOFAtEndOfFile(final File file) throws IOException {

        Bytes bytes = BytesUtil.readFile(file.getAbsolutePath());

        // to check that "00 00 00 c0") is the EOF you can run net.openhft.chronicle.queue.ChronicleQueueIndexTest.eofAsHex
        //  eofAsHex();

        // check that the EOF is in the last few bytes.
        String lastFewBytes = bytes.toHexString(131328, 128);
        //System.out.println(lastFewBytes);

        // 00 00 00 c0 is the EOF
        return lastFewBytes.contains("00 00 00 c0");
    }

    private void eofAsHex() {
        Bytes<ByteBuffer> eof = elasticByteBuffer(4);
        eof.writeInt(Wires.END_OF_DATA);
        System.out.println(eof.toHexString());
        System.out.println(eof);
    }

}