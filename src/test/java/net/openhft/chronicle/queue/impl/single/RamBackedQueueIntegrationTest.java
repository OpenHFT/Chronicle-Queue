/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.queue.ChronicleQueue;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

public class RamBackedQueueIntegrationTest {
    @Test
    public void actualTmpfsWarnsOncePerOpenWithoutCreatingCycleFiles() throws Exception {
        Path mount = Paths.get("/dev/shm");
        assumeTrue(Files.isDirectory(mount) && Files.isWritable(mount));
        assumeTrue("tmpfs".equals(Files.getFileStore(mount).type()));
        Path directory = Files.createTempDirectory(mount, "queue-storage-warning-");
        Map<ExceptionKey, Integer> events = Jvm.recordExceptions(false);
        try {
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory.toFile())
                    .blockSize(1L << 30).build()) {
                assertEquals(1L << 30, queue.blockSize());
                assertEquals(1, warningCount(events));
                try (java.util.stream.Stream<Path> files = Files.list(directory)) {
                    assertFalse(files.anyMatch(p -> p.toString().endsWith(".cq4")));
                }
            }
            assertEquals(1, warningCount(events));
            events.clear();
            try (ChronicleQueue readOnly = SingleChronicleQueueBuilder.binary(directory.toFile())
                    .readOnly(true).blockSize(1L << 30).build()) {
                assertFalse(readOnly.isClosed());
                assertEquals(0, warningCount(events));
            }
        } finally {
            Jvm.resetExceptionHandlers();
            IOTools.deleteDirWithFiles(directory.toFile());
        }
    }

    private static int warningCount(Map<ExceptionKey, Integer> events) {
        return events.entrySet().stream()
                .filter(e -> e.getKey().message().contains("RAM-backed"))
                .mapToInt(Map.Entry::getValue).sum();
    }
}
