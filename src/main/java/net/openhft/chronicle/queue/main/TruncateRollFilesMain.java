/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.internal.main.InternalTruncateRollFilesMain;

/** Explicit offline cleanup of suspected untrimmed rolls; never part of writer startup. */
public final class TruncateRollFilesMain {
    private TruncateRollFilesMain() {
    }

    /**
     * Preview by default. --offline requires all users stopped and new opens externally prevented.
     * <p>Process exit is 0 for complete, 1 for incomplete/unsafe, and 2 for invalid arguments.
     * @param args root directory, optionally followed by --offline and --min-age-seconds=N
     */
    public static void main(String[] args) {
        InternalTruncateRollFilesMain.main(args);
    }
}
