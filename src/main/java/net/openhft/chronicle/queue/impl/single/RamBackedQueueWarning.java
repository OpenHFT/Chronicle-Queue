/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Advisory check performed once during construction, never on the append path. */
final class RamBackedQueueWarning {
    static final long LARGE_BLOCK_SIZE = 1L << 30;

    private RamBackedQueueWarning() {
    }

    static void warn(Path path, long blockSize, boolean readOnly) {
        String message = warning(path, blockSize, readOnly,
                directory -> Files.getFileStore(directory.toRealPath()).type());
        if (message != null)
            Jvm.warn().on(SingleChronicleQueue.class, message);
    }

    @Nullable
    static String warning(Path path, long blockSize, boolean readOnly, StoreTypeLookup lookup) {
        if (readOnly || blockSize < LARGE_BLOCK_SIZE)
            return null;
        final String type;
        try {
            type = lookup.type(path);
        } catch (IOException | SecurityException | UnsupportedOperationException unavailable) {
            // A best-effort diagnostic must not prevent opening a queue.
            return null;
        }
        if (!"tmpfs".equalsIgnoreCase(type) && !"ramfs".equalsIgnoreCase(type))
            return null;
        return "Large queue blockSize=" + blockSize + " bytes on RAM-backed " + type
                + " storage at " + path.toAbsolutePath()
                + ". Writing or pre-touching mapped pages can consume memory (and swap on tmpfs);"
                + " mapping alone does not allocate the full block in RAM."
                + " Consider disk-backed storage or a smaller blockSize, and delete disposable queues after closing them."
                + " RAM-backed storage is not durable across reboot.";
    }

    @FunctionalInterface
    interface StoreTypeLookup {
        String type(Path path) throws IOException;
    }
}
