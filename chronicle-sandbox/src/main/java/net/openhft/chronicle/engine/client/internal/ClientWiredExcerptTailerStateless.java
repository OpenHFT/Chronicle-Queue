package net.openhft.chronicle.engine.client.internal;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.map.AbstactStatelessClient;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;
import java.util.function.Function;

import static net.openhft.chronicle.engine.client.internal.ClientWiredChronicleQueueStateless.EventId;

public class ClientWiredExcerptTailerStateless extends AbstactStatelessClient implements ExcerptTailer {

    private final Bytes<?> source = Bytes.elasticByteBuffer();
    private final Wire wire;
    long index = -1;
    private ChronicleQueue queue;
    private long cid;
    private long lastWrittenIndex;

    public ClientWiredExcerptTailerStateless(ClientWiredChronicleQueueStateless queue,
                                             ClientWiredStatelessTcpConnectionHub hub,
                                             Function<Bytes<?>, Wire> wireWrapper) {
        super(queue.name(), hub, "QUEUE", 0);
        this.queue = queue;
        this.csp = "//" + queue.name() + "?view=QUEUE";
        QueueTailerResponse qar = (QueueTailerResponse) proxyReturnMarshallable(EventId.createTailer);
        this.cid = qar.getCid();
        this.wire = wireWrapper.apply(source);
    }

    @Nullable
    @Override
    public WireIn wire() {
        return wire;
    }

    @Override
    public boolean readDocument(Consumer<WireIn> reader) {
        proxyReturnWireConsumerInOut(EventId.hasNext,
                CoreFields.reply, (WriteValue) valueOut -> {
                    WriteMarshallable writeMarshallable = w -> w.write(EventId.index).int64(index + 1);
                    valueOut.marshallable(writeMarshallable);
                },
                (Function<WireIn, Void>) w -> {
                    w.read(EventId.index).int64(x -> index = x)
                            .read(CoreFields.reply).bytes(reader);
                    return null;
                });
        return true;
    }

    @Override
    public boolean index(long l) {
        return false;
    }

    @NotNull
    @Override
    public ExcerptTailer toStart() {
        return null;
    }

    @NotNull
    @Override
    public ExcerptTailer toEnd() {
        return null;
    }

    @Override
    public ChronicleQueue chronicle() {
        return queue;
    }
}
