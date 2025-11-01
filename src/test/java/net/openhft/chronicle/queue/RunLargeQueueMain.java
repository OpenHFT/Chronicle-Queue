/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.SyncMode;

/**
 * Demonstrates the use of Chronicle Queue to write and read large datasets,
 * simulating the handling of large files with configurable message sizes.
 * <p>
 * This example writes and reads messages using a Chronicle Queue with specified
 * file and message sizes. Performance metrics for both operations are logged.
 * <p>
 * Usage: java -Dfile.size=<size_in_gb> -Dmsg.size=<message_size_in_bytes> RunLargeQueueMain [queue_path]
 */
public enum RunLargeQueueMain {
    ; // no instances allowed

    // Configurable constants with defaults
    private static final int FILE_SIZE = Integer.getInteger("file.size", 1024); // in GB
    private static final int MSG_SIZE = Integer.getInteger("msg.size", 1024);  // in bytes

    /**
     * Main entry point for the application.
     *
     * @param args Optional. The first argument specifies the path to the queue directory.
     */
    public static void main(String[] args) {
        System.out.printf("Configured file size: %d GB%n", FILE_SIZE);
        System.out.printf("Configured message size: %d B%n", MSG_SIZE);

        // Default queue path or user-specified path
        String queuePath = args.length > 0 ? args[0] : "/data/tmp/cq";

        // Create and use Chronicle Queue resources
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(queuePath)
                .blockSize((long) MSG_SIZE << 30)  // 1 GB blocks
                .syncMode(SyncMode.SYNC)          // Synchronous writes
                .build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {

            BytesStore<?, Void> writeBuffer = BytesStore.nativeStore(MSG_SIZE);
            Bytes<?> readBuffer = Bytes.allocateDirect(MSG_SIZE);

            // Main write-read performance loop
            for (int t = 1; t <= FILE_SIZE; t++) {
                long writeStart = System.currentTimeMillis();

                // Write 1 GB of data in chunks
                for (int i = 0; i < 1_000_000_000; i += MSG_SIZE) {
                    appender.writeBytes(writeBuffer);
                }
                appender.sync(); // Ensure data is flushed to disk

                long writeEnd = System.currentTimeMillis();

                // Read the same 1 GB of data
                for (int i = 0; i < 1_000_000_000; i += MSG_SIZE) {
                    readBuffer.clear();
                    tailer.readBytes(readBuffer);
                }

                long readEnd = System.currentTimeMillis();

                // Log performance metrics
                System.out.printf("%d: Took %.3f seconds to write and %.3f seconds to read 1 GB%n",
                        t, (writeEnd - writeStart) / 1e3, (readEnd - writeEnd) / 1e3);
            }
        }
    }
}
