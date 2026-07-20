/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.queue.internal.util.InternalFileUtil;
import net.openhft.chronicle.queue.internal.util.InternalTailerRetention;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Utility methods for handling files in connection with ChronicleQueue.
 * <p>Provides functions for identifying removable files, checking open file states, and determining file suffixes.
 *
 * @author Per Minborg
 * @since 5.17.34
 */
public final class FileUtil {

    private FileUtil() { }

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
     * from the given {@code baseDir} without affecting any Queue
     * process that is currently active in the given {@code baseDir}
     * reading data sequentially
     * @throws UnsupportedOperationException if this operation is not
     *                                       supported for the current platform (e.g. Windows).
     */
    @NotNull
    public static Stream<File> removableRollFileCandidates(@NotNull File baseDir) {
        return InternalFileUtil.removableRollFileCandidates(baseDir);
    }

    /**
     * Returns a Stream of roll Queue files reclaimable from the given single-queue {@code baseDir}
     * under <em>consumer-gated</em> (commitment-based) retention: a roll is reclaimable only when it
     * is both older than the last {@code keepLastCycles} cycles and already read past by
     * <em>every</em> named tailer registered against the queue.
     * <p>
     * Unlike {@link #removableRollFileCandidates(File)}, which protects only processes
     * <em>currently</em> reading the queue, this protects every registered named tailer by its
     * persisted index - so a consumer that is stopped (for example a gateway restarting) never loses
     * unread rolls. It is therefore platform-independent and safe across consumer downtime, bounded
     * only by free disk. Files are returned earliest first and can be removed in that order.
     *
     * @param baseDir        the queue directory to scan
     * @param keepLastCycles the minimum number of most-recent cycles always kept (>= 1)
     * @return a Stream of reclaimable roll files, earliest first
     */
    @NotNull
    public static Stream<File> removableRollFileCandidatesByTailerPosition(@NotNull File baseDir,
                                                                           int keepLastCycles) {
        return InternalTailerRetention.analyse(baseDir, keepLastCycles).removable().stream();
    }

    /**
     * Returns all files currently opened by any process, including the PID of the process holding the file open.
     * <p>
     * Method is only supported currently on Linux operating systems.
     *
     * @return a {@link Map} of the absolute paths to all the open files on the system, mapped to the PID holding the file open
     * @throws UnsupportedOperationException if getAllOpenFiles is not supported by the operating system
     * @throws IOException                   if an error occurs while traversing filesystem metadata for open files
     */
    public static Map<String, String> getAllOpenFiles() throws IOException {
        return InternalFileUtil.getAllOpenFiles();
    }

    /**
     * Returns if the provided {@code file} has the Chronicle Queue file
     * suffix. The current file suffix is ".cq4".
     *
     * @param file to check
     * @return if the provided {@code file} has the ChronicleQueue file
     * suffix
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
     * @param file to check
     * @return if the given {@code file } is used by any process
     * @throws UnsupportedOperationException if this operation is not
     *                                       supported for the current platform (e.g. Windows).
     */
    public static FileState state(@NotNull File file) {
        return InternalFileUtil.state(file);
    }
}
