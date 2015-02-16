package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.lang.io.MultiStoreBytes;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.BooleanValue;

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

        final BooleanValue exit = DataValueClasses.newInstance(BooleanValue.class);
        long last = chronicle.lastIndex();

        if (l == 0)
            return last > 0;

        long readUpTo = l - 1;

        for (int i = 0; i < last; i++) {
            final int j = i;

            this.readDocument(wireIn -> {

                if (readUpTo == j)
                    exit.setValue(true);

                long remaining1 = wireIn.bytes().remaining();
                wireIn.bytes().skip(remaining1);
                return null;

            });

            if (exit.getValue())
                return true;
        }

        return false;

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


