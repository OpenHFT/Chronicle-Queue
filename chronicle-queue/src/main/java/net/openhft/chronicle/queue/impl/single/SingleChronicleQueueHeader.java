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

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.time.ZonedDateTime;
import java.util.UUID;

// TODO: is padded needed ?
class SingleChronicleQueueHeader implements Marshallable {

    private enum Fields implements WireKey {
        type,
        uuid, created, user, host,
        indexCount, indexSpacing,
        writePosition, dataPosition, index2Index, lastIndex,
        roll
    }

    private enum RollFields implements WireKey {
        cycle, length, format, zoneId, nextCycle, nextCycleMetaPosition
    }

    public static final String QUEUE_TYPE = "SCV4";
    public static final String CLASS_ALIAS = "Header";
    public static final long PADDED_SIZE = 512;

    // fields which can be serialized/deserialized in the normal way.
    private String type;
    private UUID uuid;
    private ZonedDateTime created;
    private String user;
    private String host;
    private int indexCount;
    private int indexSpacing;

    // support binding to off heap memory with thread safe operations.
    private LongValue writePosition;
    private LongValue dataPosition;
    private LongValue index2Index;
    private LongValue lastIndex;

    private Roll roll;

    SingleChronicleQueueHeader(SingleChronicleQueueBuilder builder) {
        this.type = QUEUE_TYPE;
        this.uuid = UUID.randomUUID();
        this.created = ZonedDateTime.now();
        this.user = System.getProperty("user.name");
        this.host = WireUtil.hostName();

        this.indexCount = 128 << 10;
        this.indexSpacing = 64;

        // This is set to null as that it can pick up the right time the
        // first time it is used.
        this.writePosition = null;
        this.dataPosition = null;
        this.index2Index = null;
        this.lastIndex = null;
        this.roll = new Roll(builder);
    }

    LongValue writePosition() {
        return writePosition;
    }

    LongValue index2Index() {
        return index2Index;
    }

    LongValue lastIndex() {
        return lastIndex;
    }

    @Override
    public void writeMarshallable(@NotNull WireOut out) {
        out.write(Fields.type).text(type)
            .write(Fields.uuid).uuid(uuid)
            .write(Fields.writePosition).int64forBinding(WireUtil.SPB_HEADER_BYTE_SIZE)
            .write(Fields.dataPosition).int64forBinding(WireUtil.SPB_HEADER_BYTE_SIZE)
            .write(Fields.created).zonedDateTime(created)
            .write(Fields.user).text(user)
            .write(Fields.host).text(host)
            .write(Fields.indexCount).int32(indexCount)
            .write(Fields.indexSpacing).int32(indexSpacing)
            .write(Fields.index2Index).int64forBinding(0L)
            .write(Fields.lastIndex).int64forBinding(-1L)
            .write(Fields.roll).marshallable(roll);

        //out.addPadding((int) (PADDED_SIZE - out.bytes().writePosition()));
    }

    @Override
    public void readMarshallable(@NotNull WireIn in) {
        in.read(Fields.type).text(this, (o, i) -> o.type = i)
            .read(Fields.uuid).uuid(this, (o, i) -> o.uuid = i)
            .read(Fields.writePosition).int64(this.writePosition, this, (o, i) -> o.writePosition = i)
            .read(Fields.dataPosition).int64(this.dataPosition, this, (o, i) -> o.dataPosition = i)
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

    public SingleChronicleQueueHeader setWritePosition(long writeByte) {
        this.writePosition.setOrderedValue(writeByte);
        return this;
    }

    public long getDataPosition() {
        return this.dataPosition.getVolatileValue();
    }

    public SingleChronicleQueueHeader setDataPosition(long dataOffset) {
        this.dataPosition.setOrderedValue(dataOffset);
        return this;
    }

    public long incrementLastIndex() {
        return lastIndex.addAtomicValue(1);
    }

    public int getRollCycle() {
        return (int)this.roll.cycle.getVolatileValue();
    }

    public SingleChronicleQueueHeader setRollCycle(int rollCycle) {
        this.roll.cycle.setOrderedValue(rollCycle);
        return this;
    }

    private class Roll implements Marshallable {

        private LongValue cycle;
        private int length;
        private String format;
        private String zoneId;
        private LongValue nextCycle;
        private LongValue nextCycleMetaPosition;

        Roll(SingleChronicleQueueBuilder builder) {
            this.cycle = null;
            this.length = builder.rollCycleLength();
            this.format = builder.rollCycleFormat();
            this.zoneId = builder.rollCycleZoneId().getId();
            this.nextCycle = null;
            this.nextCycleMetaPosition = null;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut out) {
            out.write(RollFields.cycle).int64forBinding(-1)
                .write(RollFields.length).int32(length)
                .write(RollFields.format).text(format)
                .write(RollFields.zoneId).text(zoneId)
                .write(RollFields.nextCycle).int64forBinding(-1)
                .write(RollFields.nextCycleMetaPosition).int64forBinding(-1);
        }

        @Override
        public void readMarshallable(@NotNull WireIn in) {
            in.read(RollFields.cycle).int64(this.cycle, this, (o, i) -> o.cycle = i)
                .read(RollFields.length).int32(this, (o, i) -> o.length = i)
                .read(RollFields.format).text(this, (o, i) -> o.format = i)
                .read(RollFields.zoneId).text(this, (o, i) -> o.zoneId = i)
                .read(RollFields.nextCycle).int64(this.nextCycle, this, (o, i) -> o.nextCycle = i)
                .read(RollFields.nextCycleMetaPosition).int64(this.nextCycleMetaPosition, this, (o, i) -> o.nextCycleMetaPosition = i);
        }
    }
}
