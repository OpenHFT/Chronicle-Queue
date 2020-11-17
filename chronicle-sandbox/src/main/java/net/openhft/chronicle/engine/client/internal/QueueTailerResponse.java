package net.openhft.chronicle.engine.client.internal;

import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

public class QueueTailerResponse extends QueueAppenderResponse {
    long start;

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    @Override
    public void readMarshallable(WireIn wire) throws IllegalStateException {
        super.readMarshallable(wire);
        wire.read("start").int64(x -> start = x);
    }

    @Override
    public void writeMarshallable(WireOut wire) {
        super.writeMarshallable(wire);
        wire.write("start").int64(start);
    }

    @Override
    public String toString() {
        return "QueueAppender{" +
                "cid=" + getCid() +
                ", csp=" + getCsp() +
                ", start=" + getStart() +
                '}';
    }
}
