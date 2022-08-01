package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

@UsedViaReflection
class EchoingMicroservice extends SelfDescribingMarshallable implements Closeable, Echoing {
    transient Echoed out;
    transient boolean closed;

    public EchoingMicroservice(Echoed out) {
        this.out = out;
    }

    @Override
    public void echo(DummyData data) {
        out.echoed(data);
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
