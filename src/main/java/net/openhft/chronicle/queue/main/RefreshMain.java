/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

import java.io.File;

/**
 * Command line entry point that refreshes a queue directory listing.
 * <p>
 * The tool opens a {@link SingleChronicleQueue} for the supplied directory, invokes
 * {@link SingleChronicleQueue#refreshDirectoryListing()} and then closes the queue,
 * forcing the roll cycle cache to be rebuilt. It is primarily intended for operational
 * use when files are added or removed outside the running JVM.
 */
public final class RefreshMain {

    /**
     * Call queue.refreshDirectoryListing() on the given queue directory
     *
     * @param args the directory
     */
    public static void main(String[] args) {
        final File path = new File(args[0]);
        if (!path.isDirectory()) {
            System.err.println("Path argument must be a queue directory");
            System.exit(1);
        }

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).build()) {
            queue.refreshDirectoryListing();
        }
    }
}
