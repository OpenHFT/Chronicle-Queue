package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.Chronicle;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.lang.io.Bytes;

/**
 * Created by peter.lawrey on 30/01/15.
 */
public class SingleTailer implements ExcerptTailer {
    @Override
    public WireIn wire() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Bytes bytes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean index(long l) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean nextIndex() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExcerptTailer toStart() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExcerptTailer toEnd() {
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void finish() {
        throw new UnsupportedOperationException();
    }
}
