/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class RamBackedQueueWarningTest {
    private final Path path = Paths.get("nested", "queue");

    @Test
    public void warnsAtThresholdForBothMemoryFilesystems() {
        for (String type : new String[]{"tmpfs", "ramfs", "TMPFS"}) {
            String message = RamBackedQueueWarning.warning(path, 1L << 30, false, p -> type);
            assertNotNull(message);
            assertTrue(message.contains("1073741824"));
            assertTrue(message.contains(path.toAbsolutePath().toString()));
            assertTrue(message.contains("mapping alone does not allocate"));
            assertTrue(message.contains("delete disposable queues after closing"));
        }
    }

    @Test
    public void smallAndReadOnlyQueuesDoNotProbeFilesystem() {
        RamBackedQueueWarning.StoreTypeLookup unexpected = p -> { throw new AssertionError("unexpected probe"); };
        assertNull(RamBackedQueueWarning.warning(path, 256L << 20, false, unexpected));
        assertNull(RamBackedQueueWarning.warning(path, 512L << 20, false, unexpected));
        assertNull(RamBackedQueueWarning.warning(path, (1L << 30) - 1, false, unexpected));
        assertNull(RamBackedQueueWarning.warning(path, 1L << 30, true, unexpected));
    }

    @Test
    public void usesQueueDirectoryAndDoesNotInferTypeFromItsName() {
        assertNull(RamBackedQueueWarning.warning(Paths.get("/tmp/ramfs"), 1L << 30, false, p -> "ext4"));
        assertNotNull(RamBackedQueueWarning.warning(path, 1L << 30, false, p -> {
            assertEquals(path, p);
            return "tmpfs";
        }));
        for (String unknown : new String[]{"overlay", "unknown", "", "my-tmpfs", null})
            assertNull(RamBackedQueueWarning.warning(path, 1L << 30, false, p -> unknown));
    }

    @Test
    public void unavailableFilesystemIsNonFatal() {
        assertNull(RamBackedQueueWarning.warning(path, 1L << 30, false, p -> { throw new IOException(); }));
        assertNull(RamBackedQueueWarning.warning(path, 1L << 30, false, p -> { throw new SecurityException(); }));
        assertNull(RamBackedQueueWarning.warning(path, 1L << 30, false, p -> { throw new UnsupportedOperationException(); }));
    }
}
