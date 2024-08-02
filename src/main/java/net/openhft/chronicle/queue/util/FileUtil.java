/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.queue.internal.util.InternalFileUtil;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.Map;
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
     * Returns all files currently opened by any process, including the PID of the process holding the file open.
     * <p>
     * Method is only supported currently on Linux operating systems.
     *
     * @return a {@link Map} of the absolute paths to all the open files on the system, mapped to the PID holding the file open
     * @throws UnsupportedOperationException if getAllOpenFiles is not supported by the operating system
     * @throws IOException if an error occurs while traversing filesystem metadata for open files
     */
    public static Map<String, String> getAllOpenFiles() throws IOException {
        return InternalFileUtil.getAllOpenFiles();
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
