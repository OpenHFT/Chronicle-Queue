/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

/** Offline preflight and change detection, not a substitute for external exclusion. */
final class CycleFileIdentity {
    private final Object key;
    private final long size;
    private final java.nio.file.attribute.FileTime modified;

    private CycleFileIdentity(BasicFileAttributes attributes) {
        key = attributes.fileKey();
        size = attributes.size();
        modified = attributes.lastModifiedTime();
    }

    static void noSymlinks(Path path) throws IOException {
        Path absolute = path.toAbsolutePath();
        Path current = absolute.getRoot();
        for (Path component : absolute) {
            current = current.resolve(component);
            if (Files.isSymbolicLink(current))
                throw new IOException("REFUSED_SYMLINK: " + current);
        }
    }

    static CycleFileIdentity capture(Path path) throws IOException {
        noSymlinks(path);
        BasicFileAttributes a = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!a.isRegularFile() || a.fileKey() == null)
            throw new IOException("REFUSED_IDENTITY: " + path);
        final Object links;
        try {
            links = Files.getAttribute(path, "unix:nlink", LinkOption.NOFOLLOW_LINKS);
        } catch (UnsupportedOperationException | IllegalArgumentException e) {
            throw new IOException("REFUSED_LINK_COUNT_UNAVAILABLE: " + path, e);
        }
        if (!(links instanceof Number) || ((Number) links).longValue() != 1)
            throw new IOException("REFUSED_HARD_LINKS: " + path);
        return new CycleFileIdentity(a);
    }

    long modifiedMillis() { return modified.toMillis(); }

    void check(Path path) throws IOException {
        CycleFileIdentity now = capture(path);
        if (!key.equals(now.key) || size != now.size || !modified.equals(now.modified))
            throw new IOException("REFUSED_CHANGED_FILE: " + path);
    }

    static void requireClosed(Path path) throws IOException {
        OfflineFileUsers.requireClosed(path);
    }
}
