/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.io.IOException;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import static org.junit.Assert.*;

public class OfflineFileUsersTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @org.junit.Before public void requiresLinuxIdentities() {
        org.junit.Assume.assumeTrue("Linux inode identity parser", net.openhft.chronicle.core.OS.isLinux());
    }

    private Path proc(String line) throws IOException {
        Path root = temporary.newFolder().toPath();
        Files.createDirectories(root.resolve("123/fd"));
        Files.write(root.resolve("123/maps"), line.getBytes(StandardCharsets.UTF_8));
        return root;
    }

    private String mapping(Path file) throws IOException {
        long dev = ((Number) Files.getAttribute(file, "unix:dev")).longValue();
        long ino = ((Number) Files.getAttribute(file, "unix:ino")).longValue();
        long major = (dev >>> 8 & 0xfffL) | (dev >>> 32 & 0xfffff000L);
        long minor = (dev & 0xffL) | (dev >>> 12 & 0xffffff00L);
        return "1000-2000 r--s 00000000 " + Long.toHexString(major) + ":" + Long.toHexString(minor)
                + " " + Long.toUnsignedString(ino) + " /unrelated-name (deleted)\n";
    }

    @Test public void mappingWithoutDescriptorIsRefusedByIdentity() throws Exception {
        Path file = temporary.newFile().toPath();
        Path proc = proc(mapping(file));
        IOException e = assertThrows(IOException.class, () -> OfflineFileUsers.requireClosed(file, proc));
        assertTrue(e.getMessage(), e.getMessage().startsWith("REFUSED_FILE_STATE_MAPPED"));
    }

    @Test public void anotherInodeDoesNotBlockTheTarget() throws Exception {
        Path file = temporary.newFile().toPath();
        Path other = temporary.newFile().toPath();
        Path proc = proc(mapping(other));
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> OfflineFileUsers.requireClosed(file, proc));
    }

    @Test public void descriptorAliasIsRefusedByIdentity() throws Exception {
        Path file = temporary.newFile().toPath();
        Path proc = proc("");
        Files.createSymbolicLink(proc.resolve("123/fd/5"), file);
        IOException e = assertThrows(IOException.class, () -> OfflineFileUsers.requireClosed(file, proc));
        assertTrue(e.getMessage(), e.getMessage().startsWith("REFUSED_FILE_STATE_OPEN"));
    }

    @Test public void malformedMappingRefusesCleanup() throws Exception {
        Path file = temporary.newFile().toPath();
        Path proc = proc("unparseable mapping\n");
        IOException e = assertThrows(IOException.class, () -> OfflineFileUsers.requireClosed(file, proc));
        assertTrue(e.getMessage(), e.getMessage().startsWith("REFUSED_MAP_CHECK_UNCERTAIN"));
    }

    @Test public void unreadableInspectionRefusesCleanup() throws Exception {
        Path file = temporary.newFile().toPath();
        Path proc = proc("");
        Files.delete(proc.resolve("123/maps"));
        Files.createDirectory(proc.resolve("123/maps"));
        IOException e = assertThrows(IOException.class, () -> OfflineFileUsers.requireClosed(file, proc));
        assertTrue(e.getMessage(), e.getMessage().startsWith("REFUSED_OPEN_CHECK_UNCERTAIN"));
    }
}
