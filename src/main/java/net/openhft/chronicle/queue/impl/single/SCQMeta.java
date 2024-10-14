/*
 * Copyright 2014-2020 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class SCQMeta implements Metadata {
    @NotNull
    private final SCQRoll roll;
    private int sourceId;

    @SuppressWarnings("unused")
    @UsedViaReflection
    SCQMeta(@NotNull WireIn wire) {
        this.roll = Objects.requireNonNull(wire.read(MetaDataField.roll).typedMarshallable());
        this.sourceId = wire.bytes().readRemaining() > 0 ? wire.read(MetaDataField.sourceId).int32() : 0;
    }

    SCQMeta(@NotNull SCQRoll roll, int sourceId) {
        this.roll = roll;
        this.sourceId = sourceId;
    }

    @NotNull
    public SCQRoll roll() {
        return roll;
    }

    public int sourceId() {
        return sourceId;
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire
                .write(MetaDataField.roll).typedMarshallable(roll)
                .write(MetaDataField.sourceId).int32(this.sourceId);
    }

    @Override
    public <T extends Metadata> void overrideFrom(T metadata) {
        if (!(metadata instanceof SCQMeta))
            throw new IllegalStateException("Expected SCQMeta, got " + metadata.getClass());

        SCQMeta other = (SCQMeta) metadata;

        SCQRoll roll = other.roll;
        if (roll.epoch() != this.roll.epoch()) {
            Jvm.warn().on(getClass(), "Overriding roll epoch from existing metadata, was " + this.roll.epoch() + ", overriding to " + roll.epoch());
            this.roll.epoch(roll.epoch());
        }

        if (roll.length() != this.roll.length()) {
            Jvm.warn().on(getClass(), "Overriding roll length from existing metadata, was " + this.roll.length() + ", overriding to " + roll.length());
            this.roll.length(roll.length());
            this.roll.format(roll.format());
        }

        if (roll.rollTime() != null && !Objects.equals(roll.rollTime(), this.roll.rollTime())) {
            Jvm.warn().on(getClass(), "Overriding roll time from existing metadata, was " + this.roll.rollTime() + ", overriding to " + roll.rollTime());
            this.roll.rollTime(roll.rollTime());
        }

        if (roll.rollTimeZone() != null && !Objects.equals(roll.rollTimeZone(), this.roll.rollTimeZone())) {
            Jvm.warn().on(getClass(), "Overriding roll time zone from existing metadata, was " + this.roll.rollTimeZone() + ", overriding to " + roll.rollTimeZone());
            this.roll.rollTimeZone(roll.rollTimeZone());
        }

        if (other.sourceId != sourceId) {
            Jvm.warn().on(getClass(), "Overriding sourceId from existing metadata, was " + sourceId + ", overriding to " + other.sourceId);
            this.sourceId = other.sourceId;
        }
    }
}
