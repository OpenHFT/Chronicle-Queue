/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.LocalTime;
import java.time.ZoneId;

class SCQRoll implements Demarshallable, WriteMarshallable {
    private int length;
    @Nullable
    private String format;
    @Nullable
    private LocalTime rollTime;
    @Nullable
    private ZoneId rollTimeZone;
    private long epoch;

    /**
     * used by {@link Demarshallable}
     *
     * @param wire a wire
     */
    @UsedViaReflection
    private SCQRoll(@NotNull WireIn wire) {
        length = wire.read(RollFields.length).int32();
        format = wire.read(RollFields.format).text();
        epoch = wire.read(RollFields.epoch).int64();
        ValueIn rollTimeVIN = wire.read(RollFields.rollTime);
        if (rollTimeVIN.hasNext())
            rollTime = rollTimeVIN.time();
        String zoneId = wire.read(RollFields.rollTimeZone).text();
        if (zoneId != null)
            rollTimeZone = ZoneId.of(zoneId);
        else
            rollTimeZone = null;
    }

    SCQRoll(@NotNull RollCycle rollCycle,
            long epoch,
            @Nullable LocalTime rollTime,
            @Nullable ZoneId rollTimeZone) {
        this.length = rollCycle.length();
        this.format = rollCycle.format();
        this.epoch = epoch;
        this.rollTime = rollTime;
        this.rollTimeZone = rollTimeZone;
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(RollFields.length).int32(length)
                .write(RollFields.format).text(format)
                .write(RollFields.epoch).int64(epoch);
        if (rollTime != null)
            wire.write(RollFields.rollTime).time(rollTime);
        if (rollTimeZone != null)
            wire.write(RollFields.rollTimeZone).text(rollTimeZone.getId());

    }

    /**
     * @return an epoch offset as the number of number of milliseconds since January 1, 1970,
     * 00:00:00 GMT
     */
    public long epoch() {
        return this.epoch;
    }

    public String format() {
        return this.format;
    }

    int length() {
        return length;
    }

    @Nullable
    public LocalTime rollTime() {
        return rollTime;
    }

    @Nullable
    public ZoneId rollTimeZone() {
        return rollTimeZone;
    }

    public void length(int length) {
        this.length = length;
    }

    public void format(@Nullable String format) {
        this.format = format;
    }

    public void rollTime(@Nullable LocalTime rollTime) {
        this.rollTime = rollTime;
    }

    public void rollTimeZone(@Nullable ZoneId rollTimeZone) {
        this.rollTimeZone = rollTimeZone;
    }

    public void epoch(long epoch) {
        this.epoch = epoch;
    }

    @Override
    public String toString() {
        return "SCQRoll{" +
                "length=" + length +
                ", format='" + format + '\'' +
                ", epoch=" + epoch +
                ", rollTime=" + rollTime +
                ", rollTimeZone=" + rollTimeZone +
                '}';
    }

    enum RollFields implements WireKey {
        length, format, epoch, rollTime, rollTimeZone
    }
}
