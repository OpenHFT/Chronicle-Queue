/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.core.OS;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.*;
import java.util.Map;

/** Linux diagnostic snapshot for offline maintenance, not exclusion of future opens. */
final class OfflineFileUsers {
    private OfflineFileUsers() { }

    static void requireClosed(Path file) throws IOException {
        requireClosed(file, Paths.get("/proc"));
    }

    static void requireClosed(Path file, Path proc) throws IOException {
        if (!OS.isLinux())
            throw new IOException("REFUSED_OPEN_CHECK_UNSUPPORTED: " + file);
        Map<String, Object> identity = Files.readAttributes(file, "unix:dev,ino");
        long device = ((Number) identity.get("dev")).longValue();
        long inode = ((Number) identity.get("ino")).longValue();
        try (DirectoryStream<Path> processes = Files.newDirectoryStream(proc, p -> p.getFileName().toString().matches("[0-9]+"))) {
            for (Path process : processes) {
                try {
                    checkDescriptors(process.resolve("fd"), device, inode);
                    checkMappings(process.resolve("maps"), device, inode);
                } catch (NoSuchFileException ignored) {
                    // A process may exit during inspection. Other I/O failures refuse cleanup.
                }
            }
        } catch (DirectoryIteratorException e) {
            throw new IOException("REFUSED_OPEN_CHECK_UNCERTAIN: " + proc + ": " + e.getCause(), e.getCause());
        } catch (IOException e) {
            if (e.getMessage() != null && e.getMessage().startsWith("REFUSED_"))
                throw e;
            throw new IOException("REFUSED_OPEN_CHECK_UNCERTAIN: " + proc + ": " + e, e);
        }
    }

    private static void checkDescriptors(Path directory, long device, long inode) throws IOException {
        try (DirectoryStream<Path> descriptors = Files.newDirectoryStream(directory)) {
            for (Path descriptor : descriptors) {
                try {
                    Map<String, Object> open = Files.readAttributes(descriptor, "unix:dev,ino");
                    if (((Number) open.get("dev")).longValue() == device && ((Number) open.get("ino")).longValue() == inode)
                        throw new IOException("REFUSED_FILE_STATE_OPEN: " + descriptor);
                } catch (NoSuchFileException ignored) {
                    // An fd may close between directory enumeration and stat.
                }
            }
        }
    }

    private static void checkMappings(Path maps, long device, long inode) throws IOException {
        try (BufferedReader input = Files.newBufferedReader(maps)) {
            String line;
            while ((line = input.readLine()) != null) {
                try {
                    String[] fields = line.trim().split("\\s+", 6);
                    if (fields.length < 5 || !fields[0].matches("[0-9a-fA-F]+-[0-9a-fA-F]+")
                            || !fields[1].matches("[r-][w-][x-][ps]") || !fields[2].matches("[0-9a-fA-F]+")
                            || !fields[3].matches("[0-9a-fA-F]+:[0-9a-fA-F]+") || !fields[4].matches("[0-9]+"))
                        throw new IllegalArgumentException("Malformed maps record");
                    String[] pair = fields[3].split(":");
                    long major = Long.parseUnsignedLong(pair[0], 16), minor = Long.parseUnsignedLong(pair[1], 16);
                    if ((major | minor) >>> 32 != 0)
                        throw new IllegalArgumentException("Device number exceeds Linux dev_t component");
                    // Linux makedev encoding; compare identity, not a pathname or '(deleted)' suffix.
                    long mappedDevice = ((major & 0xfffL) << 8) | ((major & 0xfffff000L) << 32)
                            | (minor & 0xffL) | ((minor & 0xffffff00L) << 12);
                    if (mappedDevice == device && Long.parseUnsignedLong(fields[4]) == inode)
                        throw new IOException("REFUSED_FILE_STATE_MAPPED: " + maps);
                } catch (IllegalArgumentException e) {
                    throw new IOException("REFUSED_MAP_CHECK_UNCERTAIN: " + maps, e);
                }
            }
        }
    }
}
