package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WrappedWire;

/**
 * Created by peter.lawrey on 03/02/15.
 */
public class ChronicleWireIn extends WrappedWire implements WireIn {
    public ChronicleWireIn(Wire wire) {
        super(wire);
    }

    @Override
    protected WireOut thisWireOut() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected WireIn thisWireIn() {
        return this;
    }
}
