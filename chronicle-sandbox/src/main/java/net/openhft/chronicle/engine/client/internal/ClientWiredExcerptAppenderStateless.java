package net.openhft.chronicle.engine.client.internal;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.map.AbstractStatelessClient;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

import static net.openhft.chronicle.engine.client.internal.ClientWiredChronicleQueueStateless.EventId;

public class ClientWiredExcerptAppenderStateless extends AbstractStatelessClient implements ExcerptAppender {

    private final Bytes<?> source = Bytes.elasticByteBuffer();
    private final Wire wire;
    private ChronicleQueue queue;
    private long cid;
    private long lastWrittenIndex = -1;

    public ClientWiredExcerptAppenderStateless(ClientWiredChronicleQueueStateless queue,
                                               ClientWiredStatelessTcpConnectionHub hub,
                                               Function<Bytes<?>, Wire> wireWrapper) {
        super(queue.name(), hub, "QUEUE", 0);
        this.queue = queue;
        this.csp = "//" + queue.name() + "?view=QUEUE";
        QueueAppenderResponse qar = (QueueAppenderResponse) proxyReturnMarshallable(EventId.createAppender);
        this.cid = qar.getCid();
        this.wire = wireWrapper.apply(source);
    }

    @Nullable
    @Override
    public WireOut wire() {
        return wire;
    }

    @Override
    public void writeDocument(WriteMarshallable writer) {
        source.clear();
        writer.accept(wire);
        source.flip();
        lastWrittenIndex = proxyBytesReturnLong(EventId.submit, source, EventId.index);
    }

    @Override
    public long lastWrittenIndex() {
        return lastWrittenIndex;
    }

    @Override
    public ChronicleQueue chronicle() {
        return queue;
    }
}
