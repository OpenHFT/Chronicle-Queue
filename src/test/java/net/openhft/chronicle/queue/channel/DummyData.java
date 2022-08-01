package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.util.BinaryLengthLength;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.BytesInBinaryMarshallable;
import net.openhft.chronicle.wire.converter.NanoTime;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;

public class DummyData extends BytesInBinaryMarshallable {
    @NanoTime
    long timeNS;
    byte[] data;

    public long timeNS() {
        return timeNS;
    }

    public DummyData timeNS(long timeNS) {
        this.timeNS = timeNS;
        return this;
    }

    public byte[] data() {
        return data;
    }

    public DummyData data(byte[] data) {
        this.data = data;
        return this;
    }

    @Override
    public void readMarshallable(BytesIn bytes) throws IORuntimeException, BufferUnderflowException, IllegalStateException {
        timeNS = bytes.readLong();
        int len = bytes.readUnsignedShort();
        if (len == (short) -1) {
            data = null;
        } else {
            if (data == null || data.length != len)
                data = new byte[len];
            bytes.read(data);
        }
    }

    @Override
    public void writeMarshallable(BytesOut bytes) throws IllegalStateException, BufferOverflowException, BufferUnderflowException, ArithmeticException {
        bytes.writeLong(timeNS);
        if (data == null) {
            bytes.writeShort((short) -1);
        } else {
            bytes.writeUnsignedShort(data.length);
            bytes.write(data);
        }
    }

    @Override
    public BinaryLengthLength binaryLengthLength() {
        return BinaryLengthLength.LENGTH_16BIT;
    }
}
