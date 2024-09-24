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
 * Utility methods for handling files in connection with ChronicleQueue.
 * <p>Provides functions for identifying removable files, checking open file states, and determining file suffixes.
 *
 * <p><b>Note:</b> This utility class is final and cannot be instantiated.
 */
public final class FileUtil {

    // Private constructor to prevent instantiation
    private FileUtil() {}

    /**
     * Returns a Stream of roll queue files that are likely removable from the given {@code baseDir}
     * without affecting any queue process currently active in the {@code baseDir} and reading data sequentially.
     * <p>
     * Files are returned in order of creation and can be removed successively in that order. If removal of a particular
     * file fails, subsequent files must be left untouched.
     * <p><b>Warning:</b> This method is inherently non-deterministic, as new queue processes may join or leave at any time asynchronously.
     * <p>Only sequential reading is supported, as random access tailers can read at any location at any time.
     * <p>Example of how unused files can be removed:
     * <pre>{@code
     * for (File file : removableFileCandidates(baseDir).collect(Collectors.toList())) {
     *     if (!file.delete()) {
     *         break;
     *     }
     * }
     * }</pre>
     *
     * @param baseDir The directory containing queue file removal candidates
     * @return A Stream of roll queue files that are likely removable from the given {@code baseDir} without affecting any active queue process
     * @throws UnsupportedOperationException If the operation is not supported on the current platform (e.g., Windows)
     */
    @NotNull
    public static Stream<File> removableRollFileCandidates(@NotNull File baseDir) {
        return InternalFileUtil.removableRollFileCandidates(baseDir); // Delegate to internal utility
    }

    /**
     * Returns all files currently opened by any process, along with the PID of the process holding the file open.
     * <p><b>Note:</b> This method is currently supported only on Linux operating systems.
     *
     * @return A {@link Map} of absolute paths to open files on the system, mapped to the PID of the process holding the file open
     * @throws UnsupportedOperationException If the operation is not supported by the operating system
     * @throws IOException If an error occurs while traversing filesystem metadata for open files
     */
    public static Map<String, String> getAllOpenFiles() throws IOException {
        return InternalFileUtil.getAllOpenFiles(); // Delegate to internal utility
    }

    /**
     * Checks if the provided {@code file} has the Chronicle Queue file suffix (".cq4").
     *
     * @param file The file to check
     * @return {@code true} if the file has the Chronicle Queue suffix, {@code false} otherwise
     */
    public static boolean hasQueueSuffix(@NotNull File file) {
        return InternalFileUtil.hasQueueSuffix(file); // Delegate to internal utility
    }

    /**
     * Determines whether the given {@code file} is being used by any process (i.e., opened for reading or writing).
     * <p>If the open state of the given {@code file} cannot be determined, {@code true} is returned by default.
     *
     * @param file The file to check
     * @return {@code true} if the file is open by any process, or if its state cannot be determined
     * @throws UnsupportedOperationException If the operation is not supported on the current platform (e.g., Windows)
     */
    public static FileState state(@NotNull File file) {
        return InternalFileUtil.state(file); // Delegate to internal utility
    }
}
