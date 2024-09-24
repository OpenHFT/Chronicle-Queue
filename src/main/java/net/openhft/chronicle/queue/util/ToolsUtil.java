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

package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.core.Jvm;

/**
 * Utility class for tools-related functions, such as resource tracing warnings.
 * <p>This class is final and cannot be instantiated.</p>
 */
public final class ToolsUtil {

    // Private constructor to prevent instantiation
    private ToolsUtil() {
    }

    /**
     * Warns the user if resource tracing is enabled, which may eventually lead to an OutOfMemoryError (OOME).
     * <p>When running tools like {@code ChronicleReader} from the Chronicle Queue source directory, this method checks
     * if resource tracing is turned on. If it is, a warning is printed to {@code System.err}, as SLF4J may not be
     * properly set up in certain tool environments (e.g., when running shell scripts like {@code queue_reader.sh}).
     */
    public static void warnIfResourceTracing() {
        // Print a warning to System.err if resource tracing is enabled
        if (Jvm.isResourceTracing())
            System.err.println("Resource tracing is turned on - this will eventually cause an OutOfMemoryError (OOME)");
    }
}
