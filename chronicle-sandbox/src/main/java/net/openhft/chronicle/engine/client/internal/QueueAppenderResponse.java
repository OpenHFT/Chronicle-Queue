package net.openhft.chronicle.engine.client.internal;

import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

public class QueueAppenderResponse implements Marshallable {
    private long cid;
    private StringBuilder csp = new StringBuilder();

    @Override
    public void readMarshallable(WireIn wire) throws IllegalStateException {
        wire.read(CoreFields.csp).text(csp)
                .read(CoreFields.cid).int32(x -> cid = x);
    }

    @Override
    public void writeMarshallable(WireOut wire) {
        wire.write(CoreFields.csp).text(csp);
        wire.write(CoreFields.cid).int32(cid);
    }

    @Override
    public String toString() {
        return "QueueAppender{" +
                "cid=" + cid +
                ", csp=" + csp +
                '}';
    }

    public long getCid() {
        return cid;
    }

    public void setCid(long cid) {
        this.cid = cid;
    }

    public StringBuilder getCsp() {
        return csp;
    }

    public void setCsp(StringBuilder csp) {
        this.csp = csp;
    }
}
