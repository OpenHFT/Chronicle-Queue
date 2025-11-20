/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.util.ExitCodeRuntimeException;

import java.io.File;

public final class RefreshMain {

    /**
     * Call queue.refreshDirectoryListing() on the given queue directory
     *
     * @param args the directory
     */
    public static void main(String[] args) {

        final File path = new File(args[0]);
        if (!path.isDirectory()) {
            throw ExitCodeRuntimeException.orExit(1, "Path argument must be a queue directory");
        }

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).build()) {
            queue.refreshDirectoryListing();
        }
    }
}
