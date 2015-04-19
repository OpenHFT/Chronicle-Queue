/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.engine.client.internal;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub.CoreFields;
import net.openhft.chronicle.engine.client.internal.ClientWiredChronicleQueueStateless.EventId;
import net.openhft.chronicle.network.WireHandler;
import net.openhft.chronicle.network.event.EventGroup;
import net.openhft.chronicle.network.event.WireHandlers;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static net.openhft.chronicle.engine.client.internal.QueueWireHandler.Fields.reply;

/**
 * Created by Rob Austin
 */
public class QueueWireHandler implements WireHandler, Consumer<WireHandlers> {

    public static final int SIZE_OF_SIZE = ClientWiredStatelessTcpConnectionHub.SIZE_OF_SIZE;
    private static final Logger LOG = LoggerFactory.getLogger(QueueWireHandler.class);
    final StringBuilder cspText = new StringBuilder();
    final StringBuilder eventName = new StringBuilder();
    // assume there is a handler for each connection.
    long tid = -1;
    long cid = -1;
    ChronicleQueue queue = null;
    private WireHandlers publishLater;
    private Wire inWire;
    private Wire outWire;
    private Map<Long, ChronicleQueue> cidToQueue = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Integer> cspToCid = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ChronicleQueue> fileNameToChronicle = new ConcurrentHashMap<>();
    private AtomicInteger cidCounter = new AtomicInteger();
    private Map<ChronicleQueue, ExcerptAppender> queueToAppender = new ConcurrentHashMap<>();

    public QueueWireHandler() {
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

    @SuppressWarnings("UnusedReturnValue")
    void onEvent() throws IOException {
        //Be careful not to use Wires.acquireStringBuilder() as we
        //need to store the value

        Bytes[] tmpBytes = {null};

        inWire.readDocument(
                w -> {
                    w.read(CoreFields.csp).text(cspText)
                            .read(CoreFields.tid).int64(x -> tid = x)
                            .read(CoreFields.cid).int64(x -> cid = x);
                    queue = getQueue(cspText);
                }, dataWireIn -> {
                    ValueIn vin = inWire.readEventName(eventName);
                    tmpBytes[0] = NativeBytes.nativeBytes();
                    vin.bytes(tmpBytes[0]);
                });

        try {
            // writes out the tid
            outWire.writeDocument(true, wire -> outWire.write(CoreFields.tid).int64(tid));

            if (EventId.lastWrittenIndex.contentEquals(eventName)) {
                writeData(wireOut -> wireOut.write(reply).int64(queue.lastWrittenIndex()));
            } else if (EventId.createAppender.contentEquals(eventName)) {
                //only need one appender per queue
                queueToAppender.computeIfAbsent(queue,
                        s -> {
                            try {
                                return queue.createAppender();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            return null;
                        });

                outWire.writeDocument(false, wireOut -> {
                    QueueAppenderResponse qar = new QueueAppenderResponse();
                    qar.setCid(cid);
                    qar.setCsp(cspText);
                    wireOut.write(reply).typedMarshallable(qar);
                });
            } else if (EventId.submit.contentEquals(eventName)) {
                ExcerptAppender appender = queueToAppender.get(queue);

                // new BinaryWire(tmpBytes).copyTo(new TextWire(appender.wire().bytes()));
                tmpBytes[0].flip();
                System.out.println(tmpBytes[0]);
                appender.writeDocument(wo -> wo.bytes().write(tmpBytes[0]));

                long index = appender.lastWrittenIndex();

                outWire.writeDocument(false, wire -> wire.write(EventId.index).int64(appender.lastWrittenIndex()));
            }


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

    private ChronicleQueue getQueue(StringBuilder cspText) {
        ChronicleQueue queue;
        if (cid == 0) {
            //cid hasn't been passed in need to map it from csp
            cid = cspToCid.computeIfAbsent(cspText.toString(),
                    s -> cidCounter.incrementAndGet());
            String[] parts = cspText.toString().split("/");

            String filename = "/tmp/" + parts[1] + "/" + parts[2] + ".q";

            queue = fileNameToChronicle.computeIfAbsent
                    (filename, s -> {
                        try {
                            return new ChronicleQueueBuilder(filename).build();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return null;
                    });
            cidToQueue.put(cid, queue);
        } else {
            //if the cid has been created then there must be a corresponding queue
            queue = cidToQueue.get(cid);
            assert queue != null;
        }
        return queue;
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

