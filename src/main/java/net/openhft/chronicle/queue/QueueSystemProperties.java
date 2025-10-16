/*
 * Copyright 2016-2025 chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    public static boolean CHECK_INDEX = Jvm.getBoolean("queue.check.index");

    /**
     * Name of a system property used to specify the default roll cycle.
     * <p>
     * System Property key: "net.openhft.queue.builder.defaultRollCycle"
     * Fallback if unset  : to {@link net.openhft.chronicle.queue.RollCycles#DEFAULT}
     * Valid values       : Class name of an entity implementing RollCycle such as "net.openhft.chronicle.queue.harness.WeeklyRollCycle"
     *                       or enum value in class:name format such as "net.openhft.chronicle.queue.RollCycles:HOURLY"
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
