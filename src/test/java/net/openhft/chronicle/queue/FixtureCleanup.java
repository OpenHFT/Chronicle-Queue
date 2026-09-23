/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IOTools;

import java.io.File;

/** Test-only cleanup scope: try-with-resources retains a body failure and suppresses cleanup failures. */
public final class FixtureCleanup implements AutoCloseable {
    private final Runnable[] actions;

    public FixtureCleanup(Runnable... actions) {
        this.actions = actions;
    }

    public static FixtureCleanup deleting(File... paths) {
        Runnable[] actions = new Runnable[paths.length + 1];
        actions[0] = BackgroundResourceReleaser::releasePendingResources;
        for (int i = 0; i < paths.length; i++) {
            File path = paths[i];
            actions[i + 1] = () -> IOTools.deleteDirWithFilesOrThrow(path);
        }
        return new FixtureCleanup(actions);
    }

    @Override
    public void close() {
        Throwable first = null;
        for (Runnable action : actions) {
            try {
                action.run();
            } catch (RuntimeException | Error failure) {
                if (first == null)
                    first = failure;
                else if (first != failure)
                    first.addSuppressed(failure);
            }
        }
        if (first instanceof Error)
            throw (Error) first;
        if (first != null)
            throw (RuntimeException) first;
    }
}
