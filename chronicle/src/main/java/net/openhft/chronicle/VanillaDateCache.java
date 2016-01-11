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

package net.openhft.chronicle;

import net.openhft.lang.Maths;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

class VanillaDateCache {
    private static final int SIZE = 32;

    private final SimpleDateFormat format;
    private final DateValue[] values;
    private final int cycleLength;
    private final File rootPath;

    public VanillaDateCache(String rootPath, String formatStr, int cycleLength, TimeZone timeZone) {
        this(new File(rootPath), formatStr, cycleLength, timeZone);
    }

    public VanillaDateCache(File rootPath, String formatStr, int cycleLength, TimeZone timeZone) {
        this.rootPath = rootPath;
        this.cycleLength = cycleLength;
        this.values = new DateValue[SIZE];

        this.format = new SimpleDateFormat(formatStr);
        this.format.setTimeZone(timeZone);
    }

    /**
     * Formats a cycle number into a date/time String based on a fixed date/time format.
     *
     * @param cycle the cycle number to format
     *
     * @return the formatted date/time string
     */
    /*
    public String formatFor(int cycle) {
        long millis = (long) cycle * cycleLength;
        int hash = (int) Maths.hash(millis) & (SIZE - 1);
        DateValue dv = values[hash];
        if (dv == null || dv.millis != millis) {
            synchronized (format) {
                String text = format.format(new Date(millis));
                values[hash] = new DateValue(millis, text, new File(rootPath, text));
                return text;
            }
        }
        return dv.text;
    }
    */

    public DateValue valueFor(int cycle) {
        long millis = (long) cycle * cycleLength;
        int hash = (int) Maths.hash(millis) & (SIZE - 1);
        DateValue dv = values[hash];
        if (dv == null || dv.millis != millis) {
            synchronized (format) {
                String text = format.format(new Date(millis));
                return values[hash] = new DateValue(
                    millis,
                    text,
                    new File(rootPath, text));
            }
        }
        return dv;
    }

    public long parseCount(String name) throws ParseException {
        synchronized (format) {
            return format.parse(name).getTime() / cycleLength;
        }
    }

    static class DateValue {
        final long millis;
        final String text;
        final File path;

        DateValue(long millis, String text, File path) {
            this.millis = millis;
            this.text = text;
            this.path = path;
        }
    }
}
