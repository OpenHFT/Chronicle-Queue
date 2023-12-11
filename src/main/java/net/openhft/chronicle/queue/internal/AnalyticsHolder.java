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

public enum AnalyticsHolder {
    ; // none

    private static final String VERSION = PomProperties.version("net.openhft", "chronicle-queue");

    private static final AnalyticsFacade ANALYTICS = AnalyticsFacade.standardBuilder("G-4K5MBLGPLE", "k1hK3x2qQaKk4F5gL-PBhQ", VERSION)
            //.withReportDespiteJUnit()
            .withDebugLogger(System.out::println)
            //.withUrl("https://www.google-analytics.com/debug/mp/collect")
            .build();

    public static AnalyticsFacade instance() {
        return ANALYTICS;
    }
}
