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

package net.openhft.chronicle.queue.internal;

import net.openhft.chronicle.core.analytics.AnalyticsFacade;
import net.openhft.chronicle.core.pom.PomProperties;

/**
 * The {@code AnalyticsHolder} class is a utility to manage the initialization and access to an {@link AnalyticsFacade} instance.
 * <p>
 * It fetches the Chronicle Queue version and builds the analytics facade using standard configurations, including
 * a debug logger.
 * </p>
 */
public enum AnalyticsHolder {
    ; // Enum with no instances, acting as a static holder

    // Fetches the current version of Chronicle Queue from the POM properties
    private static final String VERSION = PomProperties.version("net.openhft", "chronicle-queue");

    // Builds the analytics facade with standard settings, including a debug logger
    private static final AnalyticsFacade ANALYTICS = AnalyticsFacade.standardBuilder("G-4K5MBLGPLE", "k1hK3x2qQaKk4F5gL-PBhQ", VERSION)
            //.withReportDespiteJUnit() // Uncomment to report analytics even during JUnit tests
            .withDebugLogger(System.out::println) // Logs debug information to the system output
            //.withUrl("https://www.google-analytics.com/debug/mp/collect") // Uncomment for custom analytics URL
            .build();

    /**
     * Provides access to the singleton {@link AnalyticsFacade} instance for use in reporting analytics data.
     *
     * @return The singleton {@link AnalyticsFacade} instance
     */
    public static AnalyticsFacade instance() {
        return ANALYTICS;
    }
}
