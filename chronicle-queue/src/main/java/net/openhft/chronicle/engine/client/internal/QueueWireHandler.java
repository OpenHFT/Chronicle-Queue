package net.openhft.chronicle.engine.client.internal;

import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub.CoreFields;
import net.openhft.chronicle.engine.client.internal.ClientWiredChronicleQueueStateless.EventId;
import net.openhft.chronicle.network.WireHandler;
import net.openhft.chronicle.network.event.EventGroup;
import net.openhft.chronicle.network.event.WireHandlers;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.function.Consumer;

import static net.openhft.chronicle.engine.client.internal.QueueWireHandler.Fields.reply;

/**
 * Created by Rob Austin
 */
public class QueueWireHandler implements WireHandler, Consumer<WireHandlers> {

    private static final Logger LOG = LoggerFactory.getLogger(QueueWireHandler.class);
    public static final int SIZE_OF_SIZE = ClientWiredStatelessTcpConnectionHub.SIZE_OF_SIZE;

    private WireHandlers publishLater;
    private Wire inWire;
    private Wire outWire;
    private ChronicleQueue queue;

    public QueueWireHandler(@NotNull final ChronicleQueue queue) {
        this.queue = queue;
    }

    @Override
    public void accept(WireHandlers wireHandlers) {
        this.publishLater = wireHandlers;
    }

    @Override
    public void process(Wire in, Wire out) throws StreamCorruptedException {
        try {
            this.inWire = in;
            this.outWire = out;
            onEvent();
        } catch (Exception e) {
            LOG.error("", e);
        }
    }

    long tid;

    @SuppressWarnings("UnusedReturnValue")
    void onEvent() throws IOException {

        final StringBuilder cspText = Wires.acquireStringBuilder();
        final StringBuilder eventName = Wires.acquireStringBuilder();

        inWire.readDocument(
                metaWireIn -> {
                    inWire.read(CoreFields.csp).text(cspText);
                    tid = inWire.read(CoreFields.tid).int64();
                }, dataWireIn -> {
                    inWire.readEventName(eventName);
                });

        try {


            // writes out the tid
            outWire.writeDocument(true, wire -> outWire.write(CoreFields.tid).int64(tid));

            // todo we should have a better way to get the queue
            if (EventId.lastWrittenIndex.contentEquals(eventName))
                writeData(wireOut -> wireOut.write(reply).int64(queue.lastWrittenIndex()));

            // todo add the others here

        } finally {

            if (EventGroup.IS_DEBUG) {
                long len = outWire.bytes().position() - SIZE_OF_SIZE;
                if (len == 0) {
                    System.out.println("--------------------------------------------\n" +
                            "server writes:\n\n<EMPTY>");
                } else {

                    System.out.println("--------------------------------------------\n" +
                            "server writes:\n\n" +
                            Wires.fromSizePrefixedBlobs(outWire.bytes(), SIZE_OF_SIZE, len));

                }
            }
        }
    }

    private void writeData(Consumer<WireOut> c) {

        try {
            outWire.bytes().mark();
            outWire.writeDocument(false, c);
        } catch (Exception e) {
            outWire.bytes().reset();
            final WireOut o = outWire.write(reply)
                    .type(e.getClass().getSimpleName());

            if (e.getMessage() != null)
                o.writeValue().text(e.getMessage());

            LOG.error("", e);
        }
    }


    // note : peter has asked for these to be in camel case
    public enum Fields implements WireKey {
        reply
    }
}

