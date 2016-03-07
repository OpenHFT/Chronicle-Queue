/**
 * Copyright (C) 2016  higherfrequencytrading.com
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.queue.RollCycle;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.text.ParseException;
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

    public RollingResourcesCache(@NotNull final RollCycle cycle, long epoch, @NotNull Function<String, File> fileFactory) {
        this(cycle.length(), cycle.format(), epoch, fileFactory);
    }

    private RollingResourcesCache(final int length, @NotNull String format, long epoch, @NotNull Function<String, File> fileFactory) {
        this.length = length;
        this.values = new Resource[SIZE];
        long millis = ((epoch + 43200000) % 86400000) - 43200000;
        ZoneOffset zoneOffset = ZoneOffset.ofTotalSeconds((int) (millis / 1000));
        ZoneId zoneId = ZoneId.ofOffset("GMT", zoneOffset);
        this.formatter = DateTimeFormatter.ofPattern(format).withZone(zoneId);
        this.fileFactory = fileFactory;
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

    public int parseCount(@NotNull String name) throws ParseException {
        TemporalAccessor parse = formatter.parse(name);
        long epochDay = parse.getLong(ChronoField.EPOCH_DAY) * 86400;
        if (parse.isSupported(ChronoField.SECOND_OF_DAY))
            epochDay += parse.getLong(ChronoField.SECOND_OF_DAY);
        return Maths.toInt32(epochDay / (length / 1000));
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
