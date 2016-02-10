/**
 *     Copyright (C) 2016  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.queue.RollCycle;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;
import java.util.function.Function;

public class RollingResourcesCache {
    private static final int SIZE = 32;

    @NotNull
    private final Function<String, File> fileFactory;
    @NotNull
    private final DateFormat formatter;
    @NotNull
    private final Resource[] values;
    private final int length;

    public RollingResourcesCache(@NotNull final RollCycle cycle, @NotNull Function<String, File> fileFactory) {
        this(cycle.length(), cycle.format(), cycle.zone(), fileFactory);
    }

    private RollingResourcesCache(final int length, @NotNull String format, @NotNull final ZoneId zoneId, @NotNull Function<String, File> fileFactory) {
        this.length = length;
        this.values = new Resource[SIZE];
        this.formatter = new SimpleDateFormat(format);
        this.formatter.setTimeZone(TimeZone.getTimeZone(zoneId));
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
            synchronized (formatter) {
                @NotNull String text = formatter.format(new Date(millis));
                values[hash] = dv = new Resource(millis, text, fileFactory.apply(text));
            }
        }
        return dv;
    }

    public long parseCount(@NotNull String name) throws ParseException {
        synchronized (formatter) {
            return formatter.parse(name).getTime() / length;
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
