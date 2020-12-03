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

    /**
     * Name of a system property used to specify the default roll cycle.
     * <p>
     * System Property key: "net.openhft.queue.builder.defaultRollCycle"
     * Default unset value: net.openhft.chronicle.queue.RollCycles.DEFAULT
     * Valid values       : Any name of an entity implementing RollCycle such as "net.openhft.chronicle.queue.RollCycles.MINUTELY"
     */
    public static final String DEFAULT_ROLL_CYCLE_PROPERTY = "net.openhft.queue.builder.defaultRollCycle";

    /**
     * Name of a system property used to specify the default epoch offset property.
     * <p>
     * System Property key: "net.openhft.queue.builder.defaultEpoch"
     * Default unset value: 0L
     * Valid values       : Any long value
     */
    public static final String DEFAULT_EPOCH_PROPERTY = "net.openhft.queue.builder.defaultEpoch";

    // The name space of the system properties should be managed. Eg. map.x.y, queue.a.b

}
