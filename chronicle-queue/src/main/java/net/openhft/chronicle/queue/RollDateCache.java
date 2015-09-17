/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue;


import net.openhft.chronicle.core.Maths;

import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;

public class RollDateCache {
    private static final int SIZE = 32;

    private final DateTimeFormatter formatter;
    private final DateValue[] values;
    private final int length;
    private final ZoneId zoneId;

    public RollDateCache(final int length, String format, final ZoneId zoneId) {
        this.length = length;
        this.zoneId = zoneId;
        this.values = new DateValue[SIZE];
        this.formatter = DateTimeFormatter.ofPattern(format).withZone(zoneId);
    }

    /**
     * Formats a rollCycle number into a date/time String based on a fixed date/time format.
     *
     * @param cycle the rollCycle number to format
     *
     * @return the formatted date/time string
     */
    public String formatFor(int cycle) {
        long millis = (long) cycle * length;
        int hash = (int) Maths.longHash(millis) & (SIZE - 1);
        DateValue dv = values[hash];
        if (dv == null || dv.millis != millis) {
            synchronized (formatter) {
                String text = formatter.format(Instant.ofEpochMilli(millis));
                values[hash] = new DateValue(millis, text);
                return text;
            }
        }
        return dv.text;
    }

    public long parseCount(String name) throws ParseException {
        synchronized (formatter) {
            return formatter.parse(name).get(ChronoField.INSTANT_SECONDS) / length;
        }
    }

    static class DateValue {
        final long millis;
        final String text;

        DateValue(long millis, String text) {
            this.millis = millis;
            this.text = text;
        }
    }
}
