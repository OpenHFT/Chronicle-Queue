/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.sandbox;

import net.openhft.lang.Maths;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DateCache {
    static final int SIZE = 32;
    static final TimeZone GMT = TimeZone.getTimeZone("GMT");

    final SimpleDateFormat format;
    final DateValue[] values = new DateValue[SIZE];
    final int cycleLength;

    public DateCache(String formatStr, int cycleLength) {
        this.cycleLength = cycleLength;
        format = new SimpleDateFormat(formatStr);
        format.setTimeZone(GMT);
    }

    public String formatFor(int cycle) {
        long millis = (long) cycle * cycleLength;
        int hash = Maths.hash(millis) & (SIZE - 1);
        DateValue dv = values[hash];
        if (dv == null || dv.millis != millis) {
            synchronized (format) {
                String text = format.format(new Date(millis));
                values[hash] = new DateValue(millis, text);
                return text;
            }
        }
        return dv.text;
    }

    public long parseCount(String name) throws ParseException {
        return format.parse(name).getTime() / cycleLength;
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
