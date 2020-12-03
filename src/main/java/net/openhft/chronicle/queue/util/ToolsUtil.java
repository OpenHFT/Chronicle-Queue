package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.core.Jvm;

public final class ToolsUtil {

    private ToolsUtil() {
    }

    /**
     * When running tools e.g. ChronicleReader, from the CQ source dir, resource tracing may be turned on
     */
    public static void warnIfResourceTracing() {
        // System.err (*not* logger as slf4j may not be set up e.g. when running queue_reader.sh)
        if (Jvm.isResourceTracing())
            System.err.println("Resource tracing is turned on - this will eventually die with OOME");
    }
}
