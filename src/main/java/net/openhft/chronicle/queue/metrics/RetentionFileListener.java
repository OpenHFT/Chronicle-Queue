/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.metrics;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.StoreFileListener;

import java.io.File;

/**
 * Age-based retention for a demo/dev telemetry queue: whenever a roll-cycle file is
 * released, every {@code .cq4} in the directory older than the retention window is
 * deleted. Call {@link #sweepNow()} at construction to cover files left behind by a
 * previous process (a restart never releases the old process's cycles).
 *
 * <p>Deliberately opt-in (see the {@link QueueMetricsBinding#at(java.io.File,
 * SourceSelection, net.openhft.chronicle.queue.RollCycle,
 * net.openhft.chronicle.queue.impl.StoreFileListener) listener overload}): production
 * owners decide retention deliberately, but an unattended demo must not be able to
 * fill a disk - a flat-out span-emitting demo wrote ~915GB before a 95%-full disk was
 * noticed. Pair with an hourly roll cycle so deletion granularity is useful.
 */
public final class RetentionFileListener implements StoreFileListener {

    private final File dir;
    private final long retainMillis;

    public RetentionFileListener(File dir, long retainMillis) {
        this.dir = dir;
        this.retainMillis = retainMillis;
    }

    @Override
    public void onReleased(int cycle, File file) {
        sweepNow();
    }

    /** Deletes every {@code .cq4} whose last modification is beyond the window. */
    public void sweepNow() {
        File[] files = dir.listFiles((d, name) -> name.endsWith(".cq4"));
        if (files == null)
            return;
        long cutoff = System.currentTimeMillis() - retainMillis;
        for (File f : files) {
            if (f.lastModified() < cutoff && f.delete())
                Jvm.perf().on(RetentionFileListener.class,
                        "retention: deleted " + f.getName() + " from " + dir);
        }
    }
}
