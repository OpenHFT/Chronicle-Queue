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
    public void sum(double x, double y) {
        Jvm.startup().on(SumServiceImpl.class, "Processing sum("+x+","+y+")");
        outputQ.sumResult(x+y);
    }

    public void close() {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

}
