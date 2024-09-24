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

package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.queue.util.FileUtil;

import java.io.File;

/**
 * The InternalRemovableRollFileCandidatesMain class is responsible for finding removable
 * roll file candidates in a specified directory and printing their absolute paths.
 * If no directory is provided as an argument, the current directory is used by default.
 */
public final class InternalRemovableRollFileCandidatesMain {

    /**
     * Produces a list of removable roll file candidates from the given directory
     * and prints their absolute paths to standard output, row by row.
     *
     * @param args The directory to search for roll file candidates.
     *             If no directory is provided, the current directory ("./") is used.
     */
    public static void main(String[] args) {
        final File dir;
        if (args.length == 0) {
            // Use the current directory if no argument is provided
            dir = new File(".");
        } else {
            // Use the provided directory
            dir = new File(args[0]);
        }

        // Find removable roll file candidates and print their absolute paths
        FileUtil.removableRollFileCandidates(dir)
                .map(File::getAbsolutePath)
                .forEach(System.out::println);
    }
}
