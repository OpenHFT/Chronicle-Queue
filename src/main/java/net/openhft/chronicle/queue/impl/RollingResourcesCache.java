/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.queue.RollCycle;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.function.Function;

public class RollingResourcesCache {
    private static final int CACHE_SIZE = 128;
    private static final int ONE_DAY_IN_MILLIS = 86400000;
    private static final int HALF_DAY_IN_MILLIS = ONE_DAY_IN_MILLIS / 2;

    public static final ParseCount NO_PARSE_COUNT = new ParseCount("", Integer.MIN_VALUE);

    @NotNull
    private final Function<String, File> fileFactory;
    @NotNull
    private final DateTimeFormatter formatter;
    @NotNull
    private final Resource[] values;
    private final int length;
    @NotNull
    private final Function<File, String> fileToName;
    private final int offsetTotalSeconds;
    private final String format;
    private ParseCount lastParseCount = NO_PARSE_COUNT;
    private int dayAdjustmentDueToNegativeOffset;

    public RollingResourcesCache(@NotNull final RollCycle cycle, long epoch,
                                 @NotNull Function<String, File> nameToFile,
                                 @NotNull Function<File, String> fileToName) {
        this(cycle.length(), cycle.format(), epoch, nameToFile, fileToName);
    }

    private RollingResourcesCache(final int length,
                                  @NotNull String format, long epoch,
                                  @NotNull Function<String, File> nameToFile,
                                  @NotNull Function<File, String> fileToName) {
        this.length = length;
        this.fileToName = fileToName;
        this.values = new Resource[CACHE_SIZE];
        final long millisInDay = epoch % ONE_DAY_IN_MILLIS;

        long millis = millisInDay;

        if (Math.abs(millisInDay) > HALF_DAY_IN_MILLIS) {
            if (millisInDay > 0) {
                millis = - (ONE_DAY_IN_MILLIS - millisInDay);
                dayAdjustmentDueToNegativeOffset = 1;
            } else {
                millis = ONE_DAY_IN_MILLIS - Math.abs(millisInDay);
            }
        }

        offsetTotalSeconds = (int) (millis / 1000);
        ZoneOffset zoneOffsetFromUtc = ZoneOffset.ofTotalSeconds(offsetTotalSeconds);
        ZoneId zoneId = ZoneId.ofOffset("GMT", zoneOffsetFromUtc);
        this.format = format;
        this.formatter = DateTimeFormatter.ofPattern(this.format).withZone(zoneId);
        this.fileFactory = nameToFile;

    }

    /**
     * Cache some resources for a rollCycle number.
     *
     * @param cycle the rollCycle number to format
     * @return the Resource
     */
    @NotNull
    public Resource resourceFor(long cycle) {
        long millisSinceBeginningOfEpoch = ((cycle + dayAdjustmentDueToNegativeOffset) * length);
        int hash = Maths.hash32(millisSinceBeginningOfEpoch) & (CACHE_SIZE - 1);
        Resource dv = values[hash];
        if (dv == null || dv.millis != millisSinceBeginningOfEpoch) {
            final Instant instant = Instant.ofEpochMilli(millisSinceBeginningOfEpoch);
            @NotNull String text = formatter.format(instant);
            values[hash] = dv = new Resource(millisSinceBeginningOfEpoch, text, fileFactory.apply(text));
        }
        return dv;
    }

    public int parseCount(@NotNull String name) {
        ParseCount last = this.lastParseCount;
        if (name.equals(last.name))
            return last.count;
        int count = parseCount0(name);
        lastParseCount = new ParseCount(name, count);
        return count;
    }

    private int parseCount0(@NotNull String name) {
        try {
            TemporalAccessor parse = formatter.parse(name);

            long epochDay = parse.getLong(ChronoField.EPOCH_DAY) * 86400;
            if (parse.isSupported(ChronoField.SECOND_OF_DAY))
                epochDay += parse.getLong(ChronoField.SECOND_OF_DAY);

            return Maths.toInt32(epochDay / (length / 1000)) +
                    (offsetTotalSeconds < 0 ? 1 : 0) -
                    dayAdjustmentDueToNegativeOffset;
        } catch (DateTimeParseException e) {
            throw new RuntimeException(String.format(
                    "Unable to parse %s using format %s", name, format), e);
        }
    }

    public Long toLong(File file) {
        TemporalAccessor parse = formatter.parse(fileToName.apply(file));
        if (length == ONE_DAY_IN_MILLIS) {
            return parse.getLong(ChronoField.EPOCH_DAY);
        } else
            return Instant.from(parse).toEpochMilli() / length;
    }

    static class ParseCount {
        final String name;
        final int count;

        public ParseCount(String name, int count) {
            this.name = name;
            this.count = count;
        }
    }

    public static class Resource {
        public final long millis;
        public final String text;
        public final File path;

        Resource(long millis, String text, File path) {
            this.millis = millis;
            this.text = text;
            this.path = path;
        }
    }
}
