package net.openhft.chronicle.queue.internal;

import net.openhft.chronicle.core.analytics.AnalyticsFacade;
import net.openhft.chronicle.core.pom.PomProperties;

public enum AnalyticsHolder {;

    private static final String VERSION = PomProperties.version("net.openhft", "chronicle-queue");

    private static final AnalyticsFacade ANALYTICS = AnalyticsFacade.standardBuilder("G-4K5MBLGPLE", "k1hK3x2qQaKk4F5gL-PBhQ", VERSION)
            //.withReportDespiteJUnit()
            .build();

    public static AnalyticsFacade instance() {
        return ANALYTICS;
    }

}