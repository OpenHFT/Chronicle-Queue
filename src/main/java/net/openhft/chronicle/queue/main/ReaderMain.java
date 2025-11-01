/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.ChronicleReaderMain;
import org.jetbrains.annotations.NotNull;

/**
 * ReaderMain is an entry point for displaying records from a Chronicle Queue in text format.
 * <p>This class delegates the reading and display of queue records to {@link ChronicleReaderMain}.
 */
public final class ReaderMain {

    /**
     * The main method that triggers the reading and display of queue records.
     * Delegates execution to {@link ChronicleReaderMain#main(String[])}.
     *
     * @param args Command-line arguments
     */
    public static void main(@NotNull String[] args) {
        ChronicleReaderMain.main(args);
    }
}
