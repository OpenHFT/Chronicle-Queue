/*
 *     Copyright (C) 2015  higherfrequencytrading.com
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
package net.openhft.chronicle.queue;

import java.time.ZoneId;

public enum RollCycles implements RollCycle {
    SECONDS("yyyyMMddHHmmss", 1000, ZoneId.of("UTC")),
    MINUTES("yyyyMMddHHmm", 60 * 1000, ZoneId.of("UTC")),
    HOURS("yyyyMMddHH", 60 * 60 * 1000, ZoneId.of("UTC")),
    DAYS("yyyyMMdd", 24 * 60 * 60 * 1000, ZoneId.of("UTC"));

    static final RollCycles[] VALUES = values();

    final String format;
    final int length;
    final ZoneId zone;

    RollCycles(String format, int length, ZoneId zone) {
        this.format = format;
        this.length = length;
        this.zone = zone;
    }

    @Override
    public String format() {
        return this.format;
    }

    @Override
    public int length() {
        return this.length;
    }

    @Override
    public ZoneId zone() {
        return this.zone;
    }
}
