package net.openhft.chronicle.queue.benchmark;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.FieldGroup;
import net.openhft.chronicle.core.UnsafeMemory;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.*;
import org.openjdk.jol.vm.VM;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;

import static net.openhft.chronicle.core.UnsafeMemory.UNSAFE;


public abstract class TriviallyCopyableEvent<E extends TriviallyCopyableEvent<E>> implements Event<E>, BytesMarshallable {
    @FieldGroup("header")
    transient int description = this.$description();

    @LongConversion(ServicesTimestampLongConverter.class)
    private long eventTime;

    public TriviallyCopyableEvent() {
    }

    protected abstract int $description();

    protected abstract int $start();

    protected abstract int $length();

    @Override
    public void readMarshallable(BytesIn bytes) throws IORuntimeException, BufferUnderflowException, IllegalStateException {
        int description0 = bytes.readInt();
        if (description0 != this.$description()) {
            this.carefulCopy(bytes, description0);
        } else {
        //    bytes.unsafeReadObject(this, this.$start(), this.$length());

            final long sourceAddress = bytes.addressForRead(bytes.readPosition());
            final long targetAddress = VM.current().addressOf(this) + $start();
            UNSAFE.copyMemory(sourceAddress, targetAddress, $length());
        }

    }

/*
    @Override
    public void writeMarshallable(BytesOut bytes) throws IllegalStateException, BufferOverflowException, BufferUnderflowException, ArithmeticException {
        bytes.writeInt(this.$description());
        final long sourceAddress = VM.current().addressOf(this) + $start();
        final long targetAddress = bytes.addressForWrite(bytes.writePosition());
        UNSAFE.copyMemory(sourceAddress, targetAddress, $length());
    }
*/


    @Override
    public void writeMarshallable(BytesOut bytes) throws IllegalStateException, BufferOverflowException, BufferUnderflowException, ArithmeticException {
        bytes.writeInt(this.$description());
        bytes.unsafeWriteObject(this, this.$start(), this.$length());
    }


    private void carefulCopy(BytesIn in, int description0) {
        int offset = this.$start();
        int longs0 = description0 >>> 24;
        int ints0 = description0 >>> 16 & 255;
        int shorts0 = description0 >>> 8 & 127;
        int bytes0 = description0 & 255;
        int length = longs0 * 8 + ints0 * 4 + shorts0 * 2 + bytes0;
        if (Integer.bitCount(description0) % 2 != 0 && (long) length <= in.readRemaining()) {
            int longs = this.$description() >>> 24;

            int ints;
            for (ints = 0; ints < Math.max(longs, longs0); ++ints) {
                long value = 0L;
                if (ints < longs0) {
                    value = in.readLong();
                }

                if (ints < longs) {
                    UnsafeMemory.MEMORY.writeLong(this, (long) offset, value);
                    offset += 8;
                }
            }

            ints = this.$description() >>> 16 & 255;

            int bytes;
            int shorts;
            for (shorts = 0; shorts < Math.max(ints, ints0); ++shorts) {
                bytes = 0;
                if (shorts < ints0) {
                    bytes = in.readInt();
                }

                if (shorts < ints) {
                    UnsafeMemory.MEMORY.writeInt(this, (long) offset, bytes);
                    offset += 4;
                }
            }

            shorts = this.$description() >>> 8 & 127;

            for (bytes = 0; bytes < Math.max(shorts, shorts0); ++bytes) {
                short value = 0;
                if (bytes < shorts0) {
                    value = in.readShort();
                }

                if (bytes < shorts) {
                    UnsafeMemory.MEMORY.writeShort(this, (long) offset, value);
                    offset += 2;
                }
            }

            bytes = this.$description() & 255;

            for (int i = 0; i < Math.max(bytes, bytes0); ++i) {
                byte value = 0;
                if (i < bytes0) {
                    value = in.readByte();
                }

                if (i < bytes) {
                    UnsafeMemory.MEMORY.writeByte(this, (long) offset, value);
                    ++offset;
                }
            }

        } else {
            throw new IllegalStateException("Invalid description: " + Integer.toHexString(description0) + ", length: " + length + ", remaining: " + in.readRemaining());
        }
    }


    public static Event<?> findEvent(final Object[] args) {
        if (args == null) return null;
        for (Object arg : args) {
            if (arg instanceof Event) {
                return ((Event<?>) arg);
            }
        }
        return null;
    }

    public boolean usesSelfDescribingMessage() {
        return false;
    }

    @Override
    public void reset() {
        Wires.reset(this);
    }

    @Override
    public long eventTime() {
        return eventTime;
    }

    @Override
    public E eventTime(long eventTime) {
        this.eventTime = eventTime;
        return self();
    }

    @Override
    public E eventTimeNow() {
        return this.eventTime(ServicesTimestampLongConverter.currentTime());
    }


    @SuppressWarnings("unchecked")
    private E self() {
        return (E) this;
    }


    public boolean equals(Object o) {
        return o instanceof WriteMarshallable && (this == o || Wires.isEquals(this, o));
    }

    public int hashCode() {
        return HashWire.hash32(this);
    }

    public String toString() {
        return WireType.TEXT.asString(this);
    }

}

