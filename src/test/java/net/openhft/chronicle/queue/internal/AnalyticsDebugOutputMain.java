/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.onoes.ExceptionKey;

import java.util.Map;

/**
 * Child-process entry point for {@link AnalyticsHolderTest}. It must not reference JUnit, because
 * Chronicle Analytics mutes itself whenever JUnit is on the classpath.
 */
public final class AnalyticsDebugOutputMain {
    static final String ROUTED = "analytics-debug-routed=";

    private AnalyticsDebugOutputMain() {
    }

    public static void main(String[] args) {
        final Map<ExceptionKey, Integer> recorded = Jvm.recordExceptions(true, false, false);
        AnalyticsHolder.instance();
        final boolean routed = recorded.keySet().stream()
                .anyMatch(key -> key.clazz() == AnalyticsHolder.class && key.message() != null);
        System.out.println(ROUTED + routed);
    }
}
