package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.lang.io.MultiStoreBytes;

import java.util.function.Function;

/**
 * Created by peter.lawrey on 30/01/15.
 */
public class SingleTailer implements ExcerptTailer {
    private final DirectChronicleQueue chronicle;
    long index;
    private final MultiStoreBytes bytes = new MultiStoreBytes();
    private final Wire wire = new BinaryWire(bytes);

    public SingleTailer(ChronicleQueue chronicle) {
        this.chronicle = (DirectChronicleQueue) chronicle;
        toStart();
    }

    @Override
    public WireIn wire() {
        return new ChronicleWireIn(null);
    }

    @Override
    public <T> boolean readDocument(Function<WireIn, T> reader) {
        wire.readDocument(reader);
        return true;
    }

    @Override
    public boolean index(long l) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExcerptTailer toStart() {
        index = -1;
        chronicle.index(-1L, bytes);
        return this;
    }

    @Override
    public ExcerptTailer toEnd() {
        index(chronicle.lastIndex());
        return this;
    }

    @Override
    public ChronicleQueue chronicle() {
        return chronicle;
    }
}
