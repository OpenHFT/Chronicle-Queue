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
import java.util.function.Function;

public class RollingResourcesCache {
    private static final int SIZE = 32;

    @NotNull
    private final Function<String, File> fileFactory;
    @NotNull
    private final DateTimeFormatter formatter;
    @NotNull
    private final Resource[] values;
    private final int length;

    @NotNull
    private final Function<File, String> fileToName;

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
        this.values = new Resource[SIZE];
        long millis = ((epoch + 43200000) % 86400000) - 43200000;
        ZoneOffset zoneOffset = ZoneOffset.ofTotalSeconds((int) (millis / 1000));
        ZoneId zoneId = ZoneId.ofOffset("GMT", zoneOffset);
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
        long millis = cycle * length;
        int hash = Maths.hash32(millis) & (SIZE - 1);
        Resource dv = values[hash];
        if (dv == null || dv.millis != millis) {
            @NotNull String text = formatter.format(Instant.ofEpochMilli(millis));
            values[hash] = dv = new Resource(millis, text, fileFactory.apply(text));
        }
        return dv;
    }

    public int parseCount(@NotNull String name) {
        TemporalAccessor parse = formatter.parse(name);
        long epochDay = parse.getLong(ChronoField.EPOCH_DAY) * 86400;
        if (parse.isSupported(ChronoField.SECOND_OF_DAY))
            epochDay += parse.getLong(ChronoField.SECOND_OF_DAY);
        return Maths.toInt32(epochDay / (length / 1000));
    }

    public Long toLong(File file) {
        TemporalAccessor parse = formatter.parse(fileToName.apply(file));
        if (length == 86400_000L) {
            return parse.getLong(ChronoField.EPOCH_DAY);
        } else
            return Instant.from(parse).toEpochMilli() / length;
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
