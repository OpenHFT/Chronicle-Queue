package run.chronicle.queue.channel.squareService;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

import java.io.Closeable;

public class SquareServiceImpl extends SelfDescribingMarshallable
                        implements SquareService, Closeable {

    transient boolean closed;
    private SquareServiceOut output;

    public SquareServiceImpl( SquareServiceOut out ) {
        this.output = out;
    }

    @Override
    public void value(double x) {
        Jvm.startup().on(SquareServiceImpl.class, "Processing value("+x+")");
        output.value(x*x);
    }

    public void close() {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

}
