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

public enum RollCycle {
        SECONDS ("yyyyMMddHHmmss", 1000),
        MINUTES ("yyyyMMddHHmm"  , 60 * 1000),
        HOURS   ("yyyyMMddHH"    , 60 * 60 * 1000),
        DAYS    ("yyyyMMdd"      , 24 * 60 * 60 * 1000),
    ;

    private static RollCycle[] VALUES = values();

    private final String format;
    private final int length;

    RollCycle(String format, int length) {
        this.format = format;
        this.length = length;
    }

    public static RollCycle forLength(int length) {
        for(int i=VALUES.length - 1; i >= 0; i--) {
            if(VALUES[i].length == length) {
                return VALUES[i];
            }
        }

        throw new IllegalArgumentException("Unknown value for CycleLength (" + length + ")");
    }

    public static RollCycle forFormat(String format) {
        for(int i=VALUES.length - 1; i >= 0; i--) {
            if(VALUES[i].format == format || VALUES[i].format.equals(format)) {
                return VALUES[i];
            }
        }

        throw new IllegalArgumentException("Unknown value for CycleFormat (" + format + ")");
    }

    public String format() {
        return this.format;
    }

    public int length() {
        return this.length;
    }
}
