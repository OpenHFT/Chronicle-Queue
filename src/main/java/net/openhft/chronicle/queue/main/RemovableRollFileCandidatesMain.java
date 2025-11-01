/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.internal.main.InternalRemovableRollFileCandidatesMain;

/**
 * RemovableRollFileCandidatesMain is an entry point for producing a list of removable roll file candidates from a given directory.
 * <p>This utility prints the absolute path of each removable file to the standard output, one file per row.
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
