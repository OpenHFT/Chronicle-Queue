package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;

public final class QueueSystemProperties {

    private QueueSystemProperties() {
    }

    /**
     * Returns if Chronicle Queue shall assert certain index invariants on various
     * occasions throughout the code. Setting this property to "", "yes" or "true"
     * will enable this feature. Enabling the feature will slow down execution if
     * assertions (-ea) are enabled.
     * <p>
     * System Property key: "queue.check.index"
     * Default unset value: false
     * Activation values  : "", "yes", or "true"
     */
    public static final boolean CHECK_INDEX = Jvm.getBoolean("queue.check.index");

    // The name space of the system properties should be managed. Eg. map.x.y, queue.a.b

}
