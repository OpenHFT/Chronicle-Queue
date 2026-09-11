/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.jetbrains.annotations.NotNull;

/**
 * The entry point for writing records to a Chronicle Queue.
 * This class delegates the writing task to the internal {@link net.openhft.chronicle.queue.internal.writer.ChronicleWriterMain}.
 */
public class ChronicleWriterMain {

    /**
     * Main method for executing the ChronicleWriterMain.
     * It delegates to the internal writer implementation to run the application with the given arguments.
     *
     * @param args Command-line arguments for the writer
     * @throws Exception if an error occurs during execution
     */
    public static void main(@NotNull String[] args) throws Exception {
        new net.openhft.chronicle.queue.internal.writer.ChronicleWriterMain().run(args);
    }
}
