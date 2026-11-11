/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.core.Jvm;

/**
 * Utility class for tools-related functions, such as resource tracing warnings.
 * <p>This class is final and cannot be instantiated.
 */
public final class ToolsUtil {

    private ToolsUtil() {
    }

    /**
     * Exercises Queue's indexing code once per defining classloader, on the first caller's thread.
     * Concurrent callers wait for completion. Other methods on this class do not trigger the warm-up.
     * <p>A private queue under {@code java.io.tmpdir} uses 4 MiB blocks and 64,000 verified excerpts,
     * then closes and deletes its files. Configure a suitable writable scratch directory before calling.
     * No application queue is accessed. The finite work does not impose a filesystem time limit.
     * <p>This encourages compilation of shared code; it does not guarantee a compilation tier or
     * zero allocation. Application threads and stores still need their own warm-up, including after
     * garbage collection or rolling. No explicit GC or global logging changes are performed.
     *
     * @throws IllegalStateException if warm-up or cleanup fails; subsequent calls report the same
     *                               underlying failure without retrying
     */
    public static void warmup() {
        if (IndexWarmupHolder.FAILURE != null)
            throw new IllegalStateException("Queue indexing warm-up failed; it will not be retried",
                    IndexWarmupHolder.FAILURE);
    }

    private static final class IndexWarmupHolder {
        // A non-constant field triggers JVM class-initialization synchronization, including cleanup.
        static final RuntimeException FAILURE = IndexWarmup.run();
    }

    /**
     * Warns the user if resource tracing is enabled, which may eventually lead to an OutOfMemoryError (OOME).
     * <p>When running tools like {@code ChronicleReader} from the Chronicle Queue source directory, this method checks
     * if resource tracing is turned on. If it is, a warning is printed to {@code System.err}, as SLF4J may not be
     * properly set up in certain tool environments (e.g., when running shell scripts like {@code queue_reader.sh}).
     */
    public static void warnIfResourceTracing() {
        // System.err (*not* logger as slf4j may not be set up e.g. when running queue_reader.sh)
        if (Jvm.isResourceTracing())
            System.err.println("Resource tracing is turned on - this will eventually die with OOME");
    }
}
