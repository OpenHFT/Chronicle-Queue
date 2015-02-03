package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.Chronicle;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.values.LongValue;

import java.util.function.Consumer;

/**
 * Created by peter.lawrey on 30/01/15.
 */
public class SingleAppender implements ExcerptAppender {

    private final Chronicle chronicle;
    private final LongValue writeByte;
    private final ChronicleWireOut wireOut;

    public SingleAppender(Chronicle chronicle, LongValue writeByte) {
        this.chronicle = chronicle;
        this.writeByte = writeByte;
        wireOut = new ChronicleWireOut(null);
    }

    @Override
    public WireOut wire() {
        return wireOut;
    }

    @Override
    public Bytes bytes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeDocument(Consumer<WireOut> wire) {
        long position = writeByte.getValue();

    }

    @Override
    public void startExcerpt() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void startExcerpt(long capacity) {
        throw new UnsupportedOperationException();
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
    public long index() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Chronicle chronicle() {
        return chronicle;
    }

    @Override
    public void finish() {
        throw new UnsupportedOperationException();
    }
}
