package run.chronicle.queue.channel.ucservice;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;

import java.io.Closeable;

public class UCServiceImpl extends SelfDescribingMarshallable
                        implements Closeable, UCService {

    transient UCServiceOut responder;
    transient boolean closed;

    public UCServiceImpl(UCServiceOut resp) {
        this.responder = resp;
    }

    public void upperCase(String msg) {
        System.out.println("Processing... " + msg);
        responder.respond(msg.toUpperCase());
    }

    public void close() {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }
}
