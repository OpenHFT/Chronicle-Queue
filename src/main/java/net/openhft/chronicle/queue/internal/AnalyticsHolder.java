/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal;

import net.openhft.chronicle.core.analytics.AnalyticsFacade;
import net.openhft.chronicle.core.pom.PomProperties;

/**
 * The {@code AnalyticsHolder} class is a utility to manage the initialization and access to an {@link AnalyticsFacade} instance.
 * <p>
 * It fetches the Chronicle Queue version and builds the analytics facade using standard configurations, including
 * a debug logger.
 */
public enum AnalyticsHolder {
    ; // none

    private static final String VERSION = PomProperties.version("net.openhft", "chronicle-queue");

    private static final AnalyticsFacade ANALYTICS = AnalyticsFacade.standardBuilder("G-4K5MBLGPLE", "k1hK3x2qQaKk4F5gL-PBhQ", VERSION)
            //.withReportDespiteJUnit()
            .withDebugLogger(System.out::println)
            //.withUrl("https://www.google-analytics.com/debug/mp/collect")
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
