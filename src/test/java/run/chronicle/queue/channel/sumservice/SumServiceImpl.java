package run.chronicle.queue.channel.sumservice;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

import java.io.Closeable;

public class SumServiceImpl extends SelfDescribingMarshallable
    implements Closeable,  SumService {

    transient boolean closed;
    private SumServiceOut outputQ;

    public SumServiceImpl( SumServiceOut out ) {
        this.outputQ = out;
    }

    @Override
    public void pair(double x, double y) {
        Jvm.startup().on(SumServiceImpl.class, "Processing pair("+x+","+y+")");
        outputQ.value(x+y);
    }

    public void close() {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

}
