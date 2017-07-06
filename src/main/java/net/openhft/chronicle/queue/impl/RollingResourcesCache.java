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
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class RollingResourcesCache {
    private static final int CACHE_SIZE = 32;
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

    private final long epoch;
    @NotNull
    private final Function<File, String> fileToName;
    private ParseCount lastParseCount = NO_PARSE_COUNT;

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
        this.epoch = epoch;
        this.fileToName = fileToName;
        this.values = new Resource[CACHE_SIZE];
        long millis = epoch > TimeUnit.DAYS.toMillis(1) ?
                ((epoch + HALF_DAY_IN_MILLIS) % ONE_DAY_IN_MILLIS) - HALF_DAY_IN_MILLIS :
                epoch;

        ZoneOffset zoneOffsetFromUtc = ZoneOffset.ofTotalSeconds((int) (millis / 1000));
        ZoneId zoneId = ZoneId.ofOffset("GMT", zoneOffsetFromUtc);
        this.formatter = DateTimeFormatter.ofPattern(format).withZone(zoneId);
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
        long millis = cycle * length - epoch;
        int hash = Maths.hash32(millis) & (CACHE_SIZE - 1);
        Resource dv = values[hash];
        if (dv == null || dv.millis != millis) {
            @NotNull String text = formatter.format(Instant.ofEpochMilli(millis));
            values[hash] = dv = new Resource(millis, text, fileFactory.apply(text));
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
        TemporalAccessor parse = formatter.parse(name);

        long epochDay = parse.getLong(ChronoField.EPOCH_DAY) * 86400;
        if (parse.isSupported(ChronoField.SECOND_OF_DAY))
            epochDay += parse.getLong(ChronoField.SECOND_OF_DAY);
        if (epoch > 0) {
            epochDay += epoch / 1000;
        }
        return Maths.toInt32(epochDay / (length / 1000));
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
