/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.ChronicleHistoryReaderMain;
import org.jetbrains.annotations.NotNull;

/**
 * HistoryMain is an entry point for reading {@link net.openhft.chronicle.wire.MessageHistory} from a Chronicle Queue
 * and outputting histograms related to message latencies.
 * <p>
 * The histograms provide insights into:
 * <ul>
 *   <li>Latencies for each component that has processed a message</li>
 *   <li>Latencies between each component that has processed a message</li>
 * </ul>
 *
 * @author Jerry Shea
 */
public final class HistoryMain {

    /**
     * The main method that triggers the history reading process.
     * Delegates execution to {@link ChronicleHistoryReaderMain#main(String[])}.
     *
     * @param args Command-line arguments
     */
    public static void main(@NotNull String[] args) {
        ChronicleHistoryReaderMain.main(args);
    }
}
