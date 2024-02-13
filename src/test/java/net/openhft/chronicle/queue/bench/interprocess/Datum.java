package net.openhft.chronicle.queue.bench.interprocess;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;

class Datum extends SelfDescribingMarshallable {
    public long ts = 0;
    public byte[] filler0;

    public Datum(int size) {
        filler0 = new byte[size - 8];
    }
}
