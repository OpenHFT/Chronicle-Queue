package net.openhft.chronicle.queue.internal.util;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.util.FileState;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * Utility methods for handling Files in connection with ChronicleQueue.
 *
 * @author Per Minborg
 * @since 5.17.34
 */
public final class InternalFileUtil {

    private static final Comparator<File> EARLIEST_FIRST = comparing(File::getName);

    private InternalFileUtil() {}

    /**
     * Returns a Stream of roll Queue files that are likely removable
     * from the given {@code baseDir} without affecting any Queue
     * process that is currently active in the given {@code baseDir} reading
     * data sequentially.
     * <p>
     * Files are returned in order of creation and can successively be removed
     * in that order. If the removal of a particular file fails, then subsequent
     * files must be untouched.
     * <p>
     * WARNING: This method is inherently un-deterministic as new Queue processes may
     * join or leave at any time asynchronously. Thus, it is not recommended to store
     * results produced by this method for longer periods.
     * <p>
     * Only sequential reading is supported because random access Tailers can read at
     * any location at any time.
     * <p>
     * Here is an example of how unused files can be removed:
     *
     * <pre>{@code
     *     for (File file : removableFileCandidates(baseDir).collect(Collectors.toList())) {
     *         if (!file.delete()) {
     *             break;
     *         }
     *     }
     * }</pre>
     *
     * @param baseDir containing queue file removal candidates
     * @return a Stream of roll Queue files that are likely removable
     *         from the given {@code baseDir} without affecting any Queue
     *         process that is currently active in the given {@code baseDir}
     *         reading data sequentially
     * @throws UnsupportedOperationException if this operation is not
     *         supported for the current platform (e.g. Windows).
     */
    @NotNull
    public static Stream<File> removableRollFileCandidates(@NotNull File baseDir) {
        assertOsSupported();
        final File[] files = baseDir.listFiles(InternalFileUtil::hasQueueSuffix);
        if (files == null)
            return Stream.empty();

        final List<File> sortedInitialCandidates = Stream.of(files)
            .sorted(EARLIEST_FIRST)
            .collect(toList());

        final Stream.Builder<File> builder = Stream.builder();
        for (File file : sortedInitialCandidates) {
            // If one file is not closed, discard it and the rest in the sequence
            if (state(file) != FileState.CLOSED) break;
            builder.accept(file);
        }

        return builder.build();
    }

    /**
     * Returns if the provided {@code file} has the Chronicle Queue file
     * suffix. The current file suffix is ".cq4".
     *
     * @param     file to check
     * @return    if the provided {@code file} has the ChronicleQueue file
     *            suffix
     */
    public static boolean hasQueueSuffix(@NotNull File file) {
        return file.getName().endsWith(SingleChronicleQueue.SUFFIX);
    }

    /**
     * Returns if the given {@code file } is used by any process (i.e.
     * has the file open for reading or writing).
     * <p>
     * If the open state of the given {@code file} can not be determined, {@code true }
     * is returned.
     *
     * @param    file to check
     * @return   if the given {@code file } is used by any process
     * @throws   UnsupportedOperationException if this operation is not
     *           supported for the current platform (e.g. Windows).
     */
    public static FileState state(@NotNull File file) {
        assertOsSupported();
        if (!file.exists()) return FileState.NON_EXISTENT;
        final String absolutePath = file.getAbsolutePath();
        try {
            final Process process = new ProcessBuilder("lsof", "|", "grep", absolutePath).start();
            try  (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                return reader.lines()
                    .anyMatch(l -> l.contains(absolutePath))
                    ? FileState.OPEN
                    : FileState.CLOSED;
            } finally {
                process.destroyForcibly();
            }
        } catch(IOException ignored) {
            // Do nothing
        }
        return FileState.UNDETERMINED;
    }

    /**
     * Returns if the given {@code file } is used by any process (i.e.
     * has the file open for reading or writing).
     *
     * @param    file to check
     * @return   if the given {@code file } is used by any process
     */
    // Todo: Here is a candidate for Windows. Verify that it works
    private static FileState stateWindows(@NotNull File file) {
        if (!file.exists()) return FileState.NON_EXISTENT;
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
             FileChannel fileChannel = randomAccessFile.getChannel()) {

            final FileLock fileLock = fileChannel.tryLock();
            if (fileLock != null) {
                fileLock.close();
                return FileState.CLOSED;
            }
            return FileState.OPEN;
        } catch (IOException ignored) {
            // Do nothing
        }
        return FileState.UNDETERMINED;
    }

    private static void assertOsSupported() {
        if (OS.isWindows()) {
            throw new UnsupportedOperationException("This operation is not supported under Windows.");
        }
    }
}