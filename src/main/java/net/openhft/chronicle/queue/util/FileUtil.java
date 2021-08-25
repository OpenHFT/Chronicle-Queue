package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.queue.internal.util.InternalFileUtil;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.stream.Stream;

/**
 * Utility methods for handling Files in connection with ChronicleQueue.
 *
 * @author Per Minborg
 * @since 5.17.34
 */
public final class FileUtil {

    private FileUtil() {}

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
        return InternalFileUtil.removableRollFileCandidates(baseDir);
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
        return InternalFileUtil.hasQueueSuffix(file);
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
        return InternalFileUtil.state(file);
    }
}