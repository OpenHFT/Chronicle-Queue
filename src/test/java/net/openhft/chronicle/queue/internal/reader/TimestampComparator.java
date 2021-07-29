package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.reader.Reader;
import net.openhft.chronicle.queue.reader.comparator.BinarySearchComparator;
import net.openhft.chronicle.wire.ServicesTimestampLongConverter;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;

import java.util.Objects;

public class TimestampComparator implements BinarySearchComparator {
    static final String TS = "timestamp";
    private long ts;

    @Override
    public void accept(Reader reader) {
        this.ts = ServicesTimestampLongConverter.INSTANCE.parse(Objects.requireNonNull(reader.arg()));
    }

    @Override
    public Wire wireKey() {
        Wire rv = WireType.TEXT.apply(Bytes.elasticHeapByteBuffer());
        rv.writeEventName(TS).int64(ts);
        return rv;
    }

    @Override
    public int compare(Wire wire1, Wire wire2) {
        final long readPositionO1 = wire1.bytes().readPosition();
        final long readPositionO2 = wire2.bytes().readPosition();
        try {
            final long key1 = wire1.read(TS).int64();
            final long key2 = wire2.read(TS).int64();
            return Long.compare(key1, key2);
        } finally {
            wire1.bytes().readPosition(readPositionO1);
            wire2.bytes().readPosition(readPositionO2);
        }
    }
}
