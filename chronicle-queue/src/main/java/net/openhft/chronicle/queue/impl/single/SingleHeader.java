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
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.UUID;

// TODO: is padded needed ?
class SingleHeader implements Marshallable {

    private enum Fields implements WireKey {
        type, version,
        uuid, created, user, host,
        indexCount, indexSpacing,
        writePosition, readPosition, index2Index, lastIndex,
        roll
    }

    private enum RollFields implements WireKey {
        cycle, length, format, timeZone, nextCycle, nextCycleMetaPosition
    }

    public static final String QUEUE_TYPE = "SingleChronicleQueue";
    public static final String QUEUE_VERSION = "4.0";
    public static final String CLASS_ALIAS = "Header";
    public static final long PADDED_SIZE = 512;

    // fields which can be serialized/deserialized in the normal way.
    private String type;
    private String version;
    private UUID uuid;
    private ZonedDateTime created;
    private String user;
    private String host;
    private int indexCount;
    private int indexSpacing;

    // support binding to off heap memory with thread safe operations.
    private LongValue writePosition;
    private LongValue readPosition;
    private LongValue index2Index;
    private LongValue lastIndex;

    private Roll roll;

    SingleHeader(SingleChronicleQueueBuilder builder) {
        this.type = QUEUE_TYPE;
        this.version = QUEUE_VERSION;
        this.uuid = UUID.randomUUID();
        this.created = ZonedDateTime.now();
        this.user = System.getProperty("user.name");
        this.host = OS.getHostName();

        this.indexCount = 128 << 10;
        this.indexSpacing = 64;

        // This is set to null as that it can pick up the right time the
        // first time it is used.
        this.writePosition = null;
        this.readPosition = null;
        this.index2Index = null;
        this.lastIndex = null;
        this.roll = new Roll(builder);
    }

    LongValue writePosition() {
        return this.writePosition;
    }

    LongValue index2Index() {
        return this.index2Index;
    }

    LongValue lastIndex() {
        return this.lastIndex;
    }

    int indexCount() {
        return this.indexCount;
    }

    @Override
    public void writeMarshallable(@NotNull WireOut out) {
        out.write(Fields.type).text(type)
            .write(Fields.version).text(version)
            .write(Fields.uuid).uuid(uuid)
            .write(Fields.writePosition).int64forBinding(WireUtil.HEADER_OFFSET, writePosition = out.newLongReference())
            .write(Fields.readPosition).int64forBinding(WireUtil.HEADER_OFFSET, readPosition = out.newLongReference())
            .write(Fields.created).zonedDateTime(created)
            .write(Fields.user).text(user)
            .write(Fields.host).text(host)
            .write(Fields.indexCount).int32(indexCount)
            .write(Fields.indexSpacing).int32(indexSpacing)
            .write(Fields.index2Index).int64forBinding(0L, index2Index = out.newLongReference())
            .write(Fields.lastIndex).int64forBinding(-1L, lastIndex = out.newLongReference())
            .write(Fields.roll).marshallable(roll);
    }

    @Override
    public void readMarshallable(@NotNull WireIn in) {
        in.read(Fields.type).text(this, (o, i) -> o.type = i)
            .read(Fields.version).text(this, (o, i) -> o.version = i)
            .read(Fields.uuid).uuid(this, (o, i) -> o.uuid = i)
            .read(Fields.writePosition).int64(this.writePosition, this, (o, i) -> o.writePosition = i)
            .read(Fields.readPosition).int64(this.readPosition, this, (o, i) -> o.readPosition = i)
            .read(Fields.created).zonedDateTime(this, (o, i) -> o.created = i)
            .read(Fields.user).text(this, (o, i) -> o.user = i)
            .read(Fields.host).text(this, (o, i) -> o.host = i)
            .read(Fields.indexCount).int32(this, (o, i) -> o.indexCount = i)
            .read(Fields.indexSpacing).int32(this, (o, i) -> o.indexSpacing = i)
            .read(Fields.index2Index).int64(this.index2Index, this, (o, i) -> o.index2Index = i)
            .read(Fields.lastIndex).int64(this.lastIndex, this, (o, i) -> o.lastIndex = i)
            .read(Fields.roll).marshallable(roll);
    }

    public long getWritePosition() {
        return this.writePosition.getVolatileValue();
    }

    public SingleHeader setWritePosition(long writeByte) {
        this.writePosition.setOrderedValue(writeByte);
        return this;
    }

    public SingleHeader setWritePositionIfGreater(long writePosition) {
        for(; ;) {
            long wp = getWritePosition();
            if(writePosition > wp) {
                if(this.writePosition.compareAndSwapValue(wp, writePosition)) {
                    return this;
                }
            } else {
                break;
            }
        }

        return this;
    }

    public long getReadPosition() {
        return this.readPosition.getVolatileValue();
    }

    public SingleHeader setReadPosition(long readPosition) {
        this.readPosition.setOrderedValue(readPosition);
        return this;
    }

    public long incrementLastIndex() {
        return this.lastIndex.addAtomicValue(1);
    }

    public long getLastIndex() {
        return this.lastIndex.getVolatileValue();
    }

    public int getRollCycle() {
        return (int)this.roll.cycle.getVolatileValue();
    }

    public SingleHeader setRollCycle(int rollCycle) {
        this.roll.cycle.setOrderedValue(rollCycle);
        return this;
    }

    public SingleHeader setNextCycleMetaPosition(long position) {
        this.roll.nextCycleMetaPosition.setOrderedValue(position);
        return this;
    }

    public long getNextCycleMetaPosition() {
        return this.roll.nextCycleMetaPosition.getVolatileValue();
    }

    public int getNextRollCycle() {
        return (int)this.roll.nextCycle.getVolatileValue();
    }

    public boolean casNextRollCycle(int rollCycle) {
        return this.roll.nextCycle.compareAndSwapValue(-1, rollCycle);
    }

    private class Roll implements Marshallable {

        private int length;
        private String format;
        private ZoneId zoneId;

        private IntValue cycle;
        private IntValue nextCycle;

        // LongValue is right here
        private LongValue nextCycleMetaPosition;

        Roll(SingleChronicleQueueBuilder builder) {
            this.length = builder.rollCycleLength();
            this.format = builder.rollCycleFormat();
            this.zoneId = builder.rollCycleZoneId();

            this.cycle = null;
            this.nextCycle = null;
            this.nextCycleMetaPosition = null;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut out) {
            out.write(RollFields.cycle).int32forBinding(-1, cycle = out.newIntReference())
                .write(RollFields.length).int32(length)
                .write(RollFields.format).text(format)
                .write(RollFields.timeZone).text(zoneId.getId())
                .write(RollFields.nextCycle).int32forBinding(-1, nextCycle = out.newIntReference())
                .write(RollFields.nextCycleMetaPosition).int64forBinding(-1, nextCycleMetaPosition = out.newLongReference());
        }

        @Override
        public void readMarshallable(@NotNull WireIn in) {
            in.read(RollFields.cycle).int32(this.cycle, this, (o, i) -> o.cycle = i)
                .read(RollFields.length).int32(this, (o, i) -> o.length = i)
                .read(RollFields.format).text(this, (o, i) -> o.format = i)
                .read(RollFields.timeZone).text(this, (o, i) -> o.zoneId = ZoneId.of(i))
                .read(RollFields.nextCycle).int32(this.nextCycle, this, (o, i) -> o.nextCycle = i)
                .read(RollFields.nextCycleMetaPosition).int64(this.nextCycleMetaPosition, this, (o, i) -> o.nextCycleMetaPosition = i);
        }
    }
}
