package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectStore;

import java.util.function.Consumer;

/**
 * Created by peter.lawrey on 30/01/15.
 */
public class SingleAppender implements ExcerptAppender {

    private final DirectChronicleQueue chronicle;
    private final ChronicleWireOut wireOut;
    private final Bytes buffer = DirectStore.allocateLazy(128 * 1024).bytes();
    private final Wire wire = new BinaryWire(buffer);

    public SingleAppender(ChronicleQueue chronicle) {
        this.chronicle = (DirectChronicleQueue) chronicle;
        wireOut = new ChronicleWireOut(null);
    }

    @Override
    public WireOut wire() {
        return wireOut;
    }

    @Override
    public void writeDocument(Consumer<WireOut> writer) {
        buffer.clear();
        writer.accept(wire);
        buffer.flip();
        chronicle.appendDocument(buffer);
    }

    @Override
    public boolean nextSynchronous() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nextSynchronous(boolean nextSynchronous) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long lastWrittenIndex() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChronicleQueue chronicle() {
        return chronicle;
    }
}
