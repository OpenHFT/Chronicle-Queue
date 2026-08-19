/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

/**
 * Event-driven detector of queue-file additions and removals in a queue directory, backed by a
 * {@link WatchService}.
 * <p>
 * This is the first, self-contained increment of the work requested in
 * <a href="https://github.com/OpenHFT/Chronicle-Queue/issues/924">issue #924</a>. Today
 * {@link TableDirectoryListing#refresh(boolean)} performs a full directory scan on a fixed (roughly
 * one-minute) interval; with frequent roll cycles such as {@code MINUTELY} that periodic scan can show
 * up as a latency spike. The end goal is to drive the refresh from filesystem events and keep the
 * interval only as a fallback.
 * <p>
 * This class provides the building block: {@link #hasPendingChange()} reports, without blocking,
 * whether any {@code .cq4} file has been created or deleted in the watched directory since the previous
 * call. A future increment can consult it inside {@code refresh(false)} to skip the scan when nothing
 * changed (and still fall back to the interval-driven scan for safety and to cover
 * {@link java.nio.file.StandardWatchEventKinds#OVERFLOW OVERFLOW} / unsupported platforms).
 * <p>
 * <b>Scope / limitations of this increment:</b>
 * <ul>
 *     <li>Not yet wired into {@code refresh()} — it is an opt-in building block only.</li>
 *     <li>Only the queue directory itself is watched (queue files are flat within it), not recursively.</li>
 *     <li>Platform behaviour varies: Linux uses inotify (low latency); macOS falls back to a polling
 *     watch service with multi-second latency; Windows semantics differ again. Callers must retain the
 *     interval-driven refresh as a fallback rather than relying on events alone.</li>
 * </ul>
 * Instances are intended to be used by a single thread.
 */
public final class QueueDirectoryWatcher implements Closeable {

    private final String suffix;
    private final WatchService watchService;
    private WatchKey watchKey;
    private boolean closed;

    /**
     * Watches the given directory for creation/deletion of Chronicle Queue ({@code .cq4}) files.
     *
     * @param directory the queue directory to watch
     * @throws IOException if a watch service cannot be created or the directory cannot be registered
     */
    public QueueDirectoryWatcher(@NotNull final Path directory) throws IOException {
        this(directory, SingleChronicleQueue.SUFFIX);
    }

    /**
     * Watches the given directory for creation/deletion of files with the given suffix.
     *
     * @param directory the directory to watch
     * @param suffix    the file suffix that identifies relevant files (e.g. {@code .cq4})
     * @throws IOException if a watch service cannot be created or the directory cannot be registered
     */
    public QueueDirectoryWatcher(@NotNull final Path directory, @NotNull final String suffix) throws IOException {
        this.suffix = suffix;
        this.watchService = directory.getFileSystem().newWatchService();
        this.watchKey = directory.register(watchService, ENTRY_CREATE, ENTRY_DELETE);
    }

    /**
     * Reports, without blocking, whether any file with the configured suffix has been created or deleted
     * in the watched directory since the previous call, draining any queued events.
     * <p>
     * An {@link java.nio.file.StandardWatchEventKinds#OVERFLOW OVERFLOW} event (the watch service lost
     * track of changes) is conservatively reported as a change so the caller falls back to a full scan.
     *
     * @return true if a relevant change was observed, false otherwise
     */
    public boolean hasPendingChange() {
        if (closed || watchKey == null)
            return false;

        boolean changed = false;
        WatchKey key;
        while ((key = watchService.poll()) != null) {
            for (WatchEvent<?> event : key.pollEvents()) {
                final WatchEvent.Kind<?> kind = event.kind();
                if (kind == OVERFLOW) {
                    changed = true;
                    continue;
                }
                final Object context = event.context();
                if (context instanceof Path && ((Path) context).toString().endsWith(suffix))
                    changed = true;
            }
            if (!key.reset()) {
                // the directory is no longer accessible / the key is no longer valid
                watchKey = null;
                changed = true;
                break;
            }
        }
        return changed;
    }

    @Override
    public void close() {
        if (closed)
            return;
        closed = true;
        try {
            watchService.close();
        } catch (IOException e) {
            Jvm.warn().on(getClass(), "Failed to close watch service", e);
        }
    }
}
