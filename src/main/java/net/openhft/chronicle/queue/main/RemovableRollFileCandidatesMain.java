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

package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.internal.main.InternalRemovableRollFileCandidatesMain;

/**
 * RemovableRollFileCandidatesMain is an entry point for producing a list of removable roll file candidates from a given directory.
 * <p>This utility prints the absolute path of each removable file to the standard output, one file per row.</p>
 */
public final class RemovableRollFileCandidatesMain {

    /**
     * The main method that generates and prints the list of removable roll file candidates.
     * Delegates execution to {@link InternalRemovableRollFileCandidatesMain#main(String[])}.
     *
     * @param args The directory path to search for removable roll files. If no directory is provided, the current directory ("." ) is assumed.
     */
    public static void main(String[] args) {
        InternalRemovableRollFileCandidatesMain.main(args);
    }
}
