/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
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

/**
 * A utility class for managing Chronicle Queue related system properties.
 * This class defines constants for various system property keys and provides mechanisms
 * for configuring aspects of Chronicle Queue behavior via system properties.
 * <p>
 * The class cannot be instantiated and only contains static fields and methods.
 */
public final class QueueSystemProperties {

    // Private constructor to prevent instantiation of the utility class
    private QueueSystemProperties() {
    }

    /**
     * Indicates whether Chronicle Queue should assert certain index invariants at various points in the code.
     * <p>
     * This system property can be used to enable index checking, which will slow down execution if assertions are enabled (-ea).
     * The feature is enabled by setting the system property "queue.check.index" to one of the following values: "", "yes", or "true".
     * <p>
     * System Property key: "queue.check.index" <br>
     * Default unset value: false <br>
     * Activation values  : "", "yes", or "true"
     *
     * @see Jvm#getBoolean(String) for more details on how boolean properties are evaluated.
     */
    public static boolean CHECK_INDEX = Jvm.getBoolean("queue.check.index");

    /**
     * The system property key used to specify the default roll cycle for a Chronicle Queue.
     * <p>
     * This property allows configuration of a custom roll cycle by setting the property to either:
     * <ul>
     *   <li>The class name of an entity implementing {@link net.openhft.chronicle.queue.RollCycle}, e.g., "net.openhft.chronicle.queue.harness.WeeklyRollCycle".</li>
     *   <li>An enum value in "class:name" format, e.g., "net.openhft.chronicle.queue.RollCycles:HOURLY".</li>
     * </ul>
     * <p>
     * System Property key: "net.openhft.queue.builder.defaultRollCycle" <br>
     * Fallback if unset: {@link net.openhft.chronicle.queue.RollCycles#DEFAULT}
     */
    public static final String DEFAULT_ROLL_CYCLE_PROPERTY = "net.openhft.queue.builder.defaultRollCycle";

    /**
     * The system property key used to specify the default epoch offset for Chronicle Queue timestamps.
     * <p>
     * This property can be set to any long value, representing the epoch offset in milliseconds.
     * The default value is 0L if the property is not set.
     * <p>
     * System Property key: "net.openhft.queue.builder.defaultEpoch" <br>
     * Default unset value: 0L <br>
     * Valid values: Any long value
     */
    public static final String DEFAULT_EPOCH_PROPERTY = "net.openhft.queue.builder.defaultEpoch";

    // The name space of the system properties should be managed. Eg. map.x.y, queue.a.b

}
