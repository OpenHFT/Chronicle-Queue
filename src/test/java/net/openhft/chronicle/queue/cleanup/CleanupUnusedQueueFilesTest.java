package net.openhft.chronicle.queue.cleanup;

import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.Pretoucher;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CleanupUnusedQueueFilesTest extends ChronicleQueueTestBase {

@Test
    public void testTailingWithEmptyCycles() {
        testTailing(p -> {
            try {
                p.execute();
            } catch (InvalidEventHandlerException e) {
                e.printStackTrace();
            }
            return 1;
        });
    }

    @Test
    public void testTailingWithMissingCycles() {
        testTailing(p -> 0);
    }

    private void testTailing(Function<Pretoucher, Integer> createGap) {
        final SetTimeProvider tp = new SetTimeProvider(0);
        final File tmpDir = getTmpDir();

        try (SingleChronicleQueue queue = builder(tmpDir, WireType.BINARY).rollCycle(RollCycles.TEST_SECONDLY).timeProvider(tp).build();
             Pretoucher pretoucher = new Pretoucher(queue, null, c -> {
             }, true, true)) {
            int cyclesAdded = 0;
            ExcerptAppender appender = queue.acquireAppender();

            appender.writeText("0"); // to file ...000000
            assertEquals(1, listCQ4Files(tmpDir).length);

            tp.advanceMillis(1000);
            appender.writeText("1"); // to file ...000001
            assertEquals(2, listCQ4Files(tmpDir).length);

            tp.advanceMillis(2000);
            cyclesAdded += createGap.apply(pretoucher);
            assertEquals(2 + cyclesAdded, listCQ4Files(tmpDir).length);

            tp.advanceMillis(1000);
            appender.writeText("2"); // to file ...000004
            assertEquals(3 + cyclesAdded, listCQ4Files(tmpDir).length);

            tp.advanceMillis(2000);
            cyclesAdded += createGap.apply(pretoucher);
            assertEquals(3 + cyclesAdded, listCQ4Files(tmpDir).length);

            // now tail them all back
            int count = 0;
            ExcerptTailer tailer = queue.createTailer();
            long[] indexes = new long[3];
            while (true) {
                String text = tailer.readText();
                if (text == null)
                    break;
                indexes[count] = tailer.index() - 1;
                assertEquals(count++, Integer.parseInt(text));
            }
            assertEquals(indexes.length, count);

            // now make sure we can go direct to each index (like afterLastWritten)
            tailer.toStart();
            for (int i = 0; i < indexes.length; i++) {
                assertTrue(tailer.moveToIndex(indexes[i]));
                String text = tailer.readText();
                assertEquals(i, Integer.parseInt(text));
            }
        }
    }

    @Nullable
    private File[] listCQ4Files(File tmpDir) {
        return tmpDir.listFiles(file -> file.getName().endsWith("cq4"));
    }

    @NotNull
    protected SingleChronicleQueueBuilder builder(@NotNull File file, @NotNull WireType wireType) {
        return SingleChronicleQueueBuilder.builder(file, wireType).rollCycle(RollCycles.TEST4_DAILY).testBlockSize();
    }

    /**
     * Returns a Stream of Files that are likely to be removable
     * from the given {@code baseDir} without affecting any Queue process
     * that is currently active.
     * <p>
     * Files are returned in order of creation and can successively be removed
     * in that order. If the removal of a particular file fails, then subsequent
     * files must be untouched.
     * <p>
     * WARNING: This method is inherently un-deterministic as new Queue processes may
     * join or leave at any time asynchronously. Thus, it is not recommended to store
     * results produced by this method for longer periods.
     *
     * @param baseDir containing queue file removal candidates
     * @return a Stream of Files that are likely to be removable
     * from the given {@code baseDir} without affecting any Queue process
     * that is currently active
     */
    @NotNull
    private Stream<File> removableFileCandidates(@NotNull File baseDir) {
        final File[] files = baseDir.listFiles(this::isChronicleQueue);
        if (files == null) {
            return Stream.empty();
        }
        return Stream.of(files)
                .filter(this::isNotOpenByAnyProcess)
                .sorted();
    }

    boolean isNotOpenByAnyProcess(@NotNull File file) {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
             FileChannel fileChannel = randomAccessFile.getChannel()) {

            final FileLock fileLock = fileChannel.tryLock();
            if (fileLock != null) {
                fileLock.close();
                return true;
            }
        } catch (IOException ignored) {
        }
        return false;
    }

    private boolean isChronicleQueue(@NotNull File file) {
        return file.getName().endsWith("cq4");
    }
}