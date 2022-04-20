package net.openhft.chronicle.queue.internal.streaming;

import net.openhft.chronicle.core.Jvm;

public final class IncubatorWarning {

    static {
        Jvm.warn().on(IncubatorWarning.class,
                "The incubating features are subject to change at any time for any reason and shall\n" +
                        " not be used in production code. See net.openhft.chronicle.queue.incubator.package-info.java for details.");
    }

    public static void warnOnce() {
        // trigger static code
    }

}