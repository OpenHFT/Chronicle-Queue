package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.lang.io.MultiStoreBytes;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.LongValue;

import java.util.function.Function;

import static net.openhft.chronicle.queue.impl.Indexer.IndexOffset.toAddress0;
import static net.openhft.chronicle.queue.impl.Indexer.IndexOffset.toAddress1;

/**
 * Created by peter.lawrey on 30/01/15.
 */
public class SingleTailer implements ExcerptTailer {
    private final SingleChronicleQueue chronicle;
    long index;
    private final MultiStoreBytes bytes = new MultiStoreBytes();
    private final Wire wire = new BinaryWire(bytes);

    public SingleTailer(ChronicleQueue chronicle) {
        this.chronicle = (SingleChronicleQueue) chronicle;
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
    public boolean index(final long index) {

        long address0 = chronicle.indexToIndex() + toAddress0(index);
        long address1 = chronicle.bytes().readVolatileLong(address0);
        long start = 0;

        if (address1 != 0) {
            long address3 = chronicle.bytes().readVolatileLong(address1 + toAddress1(index));
            if (address3 != 0) {
                wire.bytes().position(address3);
                start = ((long) (index / 64L)) * 64L;
            }
        }


        final LongValue position = DataValueClasses.newInstance(LongValue.class);
        long last = chronicle.lastIndex();


        // linear scan the last part
        for (long i = start; i < last; i++) {
            final long j = i;

            Function<WireIn, Object> reader = wireIn -> {

                if (index == j)
                    position.setValue(wire.bytes().position() - 4);

                wireIn.bytes().skip(wireIn.bytes().remaining());
                return null;

            };

            wire.readDocument(reader);

            if (position.getValue() != 0) {
                wire.bytes().position(position.getValue());
                return true;
            }
        }

        return false;

    }

    @Override
    public ExcerptTailer toStart() {
        index = -1;
        chronicle.index(-1L, bytes );
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


