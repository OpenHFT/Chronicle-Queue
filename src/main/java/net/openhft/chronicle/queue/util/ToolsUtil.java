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
