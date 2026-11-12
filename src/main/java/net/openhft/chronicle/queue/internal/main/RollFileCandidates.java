/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

import net.openhft.chronicle.queue.impl.RollingResourcesCache;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.channels.FileChannel;
import java.util.TreeMap;
import java.util.stream.Stream;

/** Validate all roll names before changing any file in the directory. */
final class RollFileCandidates {
    final TreeMap<Integer, Path> files = new TreeMap<>();
    Path newest;

    private RollFileCandidates() {
    }

    static RollFileCandidates read(Path directory) throws IOException {
        CycleFileIdentity.noSymlinks(directory);
        if (!Files.isRegularFile(directory.resolve("metadata.cq4t"), LinkOption.NOFOLLOW_LINKS))
            throw new IOException("SKIPPED_NO_METADATA");
        CycleFileIdentity.capture(directory.resolve("metadata.cq4t"));
        CycleFileIdentity.requireClosed(directory.resolve("metadata.cq4t"));
        RollFileCandidates inventory = new RollFileCandidates();
        Integer newestCycle = null;
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory.toFile()).readOnly(true).build();
             Stream<Path> entries = Files.list(directory)) {
            if (queue.rollCycle().lengthInMillis() > 86400000 || queue.rollCycle().lengthInMillis() < 1000)
                throw new IOException("Unsupported roll interval for offline cleanup: " + queue.rollCycle());
            RollingResourcesCache cache = new RollingResourcesCache(queue.rollCycle(), queue.epoch(),
                    name -> directory.resolve(name + SingleChronicleQueue.SUFFIX).toFile(),
                    file -> file.getName().substring(0, file.getName().length() - SingleChronicleQueue.SUFFIX.length()));
            for (Path path : (Iterable<Path>) entries.filter(p -> p.toString().endsWith(SingleChronicleQueue.SUFFIX))::iterator) {
                if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                    throw new IOException("Unsupported roll entry: " + path);
                String name = path.getFileName().toString();
                String stem = name.substring(0, name.length() - SingleChronicleQueue.SUFFIX.length());
                int cycle = cache.parseCount(stem);
                if (!cache.resourceFor(cycle).text.equals(stem))
                    throw new IOException("Noncanonical roll name: " + name);
                CycleFileIdentity identity = CycleFileIdentity.capture(path);
                if (Files.size(path) > 0) {
                    CycleFileIdentity.requireClosed(path);
                    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
                        identity.check(path);
                        CycleFileEnd.headerEnd(channel);
                        identity.check(path);
                    }
                    if (newestCycle == null || cycle > newestCycle) {
                        newestCycle = cycle;
                        inventory.newest = path;
                    }
                }
                identity.check(path);
                if (inventory.files.put(cycle, path) != null)
                    throw new IOException("Duplicate cycle: " + cycle);
            }
        }
        return inventory;
    }
}
