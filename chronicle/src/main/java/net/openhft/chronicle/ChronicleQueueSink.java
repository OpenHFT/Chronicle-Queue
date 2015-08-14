/*
 * Copyright 2014 Higher Frequency Trading
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
package net.openhft.chronicle;

import net.openhft.chronicle.tcp.*;
import net.openhft.chronicle.tcp.network.SessionDetailsProvider;
import net.openhft.chronicle.tcp.network.TcpHandler;
import net.openhft.chronicle.tools.ResizableDirectByteBufferBytes;
import net.openhft.chronicle.tools.WrappedChronicle;
import net.openhft.chronicle.tools.WrappedExcerpt;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import static net.openhft.chronicle.tcp.AppenderAdapters.createAdapter;

class ChronicleQueueSink extends WrappedChronicle {
    private final SinkTcp sinkTcp;
    private final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;
    private final boolean isLocal;
    private final int readSpinCount;
    private volatile boolean closed;
    private ExcerptCommon excerpt;

    ChronicleQueueSink(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, final SinkTcp sinkTcp) {
        super(builder.chronicle());
        this.sinkTcp = sinkTcp;
        this.builder = builder.clone();
        this.closed = false;
        this.isLocal = builder.sharedChronicle() && sinkTcp.isLocalhost();
        this.excerpt = null;
        this.readSpinCount = builder.readSpinCount();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            if (this.sinkTcp != null) {
                this.sinkTcp.close();
            }
        }

        super.close();
    }

    @Override
    public Excerpt createExcerpt() throws IOException {
        return (Excerpt) createExcerpt0();
    }

    @Override
    public synchronized ExcerptTailer createTailer() throws IOException {
        return (ExcerptTailer) createExcerpt0();
    }

    @Override
    public ExcerptAppender createAppender() throws IOException {
        throw new UnsupportedOperationException();
    }

    private ExcerptCommon createExcerpt0() throws IOException {
        if (this.excerpt != null) {
            throw new IllegalStateException("An excerpt has already been created");
        }

        this.excerpt = isLocal
                ? new StatefulLocalExcerpt(wrappedChronicle.createTailer())
                : new StatefulExcerpt(wrappedChronicle.createTailer());

        return this.excerpt;
    }

    // *************************************************************************
    //
    // *************************************************************************

    private abstract class AbstractStatefulExcerpt extends WrappedExcerpt {
        protected final Logger logger;
        protected final ResizableDirectByteBufferBytes writeBuffer;
        protected final Bytes bytesIn;

        protected AbstractStatefulExcerpt(final ExcerptCommon excerpt) {
            super(excerpt);

            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + sinkTcp.toString());
            this.writeBuffer = new ResizableDirectByteBufferBytes(builder.minBufferSize());
            this.bytesIn = ByteBufferBytes.wrap(ChronicleTcp.createBuffer(builder.minBufferSize()));
        }

        @Override
        public boolean nextIndex() {
            return super.nextIndex() || (readNext() && super.nextIndex());
        }

        @Override
        public boolean index(long index) throws IndexOutOfBoundsException {
            return super.index(index) || (index >= 0 && readNext() && super.index(index));
        }

        @Override
        public synchronized void close() {
            try {
                sinkTcp.close();
            } catch (IOException e) {
                logger.warn("Error closing socketChannel", e);
            }

            super.close();
            ChronicleQueueSink.this.excerpt = null;
        }

        protected boolean readNext() {
            return !closed && sinkTcp.connect() && readNextExcerpt();
        }

        protected boolean readNextExcerpt() {
            try {
                if (!closed) {
                    return doReadNextExcerpt();
                }
            } catch (IOException e) {
                if (!closed) {
                    builder.connectionListener().onError(sinkTcp.socketChannel(), e);
                }

                try {
                    sinkTcp.close();
                    builder.connectionListener().onDisconnect(sinkTcp.socketChannel(), e.getMessage());
                } catch (IOException e2) {
                    logger.warn("Error closing socketChannel", e2);
                }
            }

            return false;
        }

        protected abstract boolean doReadNextExcerpt() throws IOException;
    }

    // *************************************************************************
    // STATEFUL
    // *************************************************************************

    private class StatefulLocalExcerpt extends AbstractStatefulExcerpt {

        private StatefulLocalExcerptTcpHandler tcpHandler = new StatefulLocalExcerptTcpHandler();

        public StatefulLocalExcerpt(final ExcerptCommon common) {
            super(common);
            sinkTcp.setSinkTcpHandler(tcpHandler);
            sinkTcp.addConnectionListener(new TcpConnectionHandler() {
                @Override
                public void onConnect(SocketChannel channel) {
                    bytesIn.clear();
                    bytesIn.limit(0);
                }
            });
        }

        private boolean excerptNotRead(boolean busy, int attempts) {
            return (!busy && (attempts < readSpinCount || readSpinCount == -1));
        }

        @Override
        protected boolean doReadNextExcerpt() throws IOException {
            tcpHandler.query(wrappedChronicle.lastIndex());
            sinkTcp.write();

            int attempts = 0;
            boolean busy;
            do {
                busy = sinkTcp.read();
            } while (excerptNotRead(busy, attempts++));

            return tcpHandler.state == TcpHandlerState.EXCERPT_COMPLETE;
        }

        private class StatefulLocalExcerptTcpHandler implements TcpHandler {

            private final BusyChecker busyChecker = new BusyChecker();

            private long queryIndex;

            private boolean queryRequired;

            private TcpHandlerState state;

            @Override
            public boolean process(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
                busyChecker.mark(in, out);
                processIncoming(in);
                processOutgoing(out);
                return busyChecker.busy(in, out);
            }

            private void processOutgoing(Bytes out) {
                if (queryRequired) {
                    state = TcpHandlerState.EXCERPT_NOT_FOUND;
                    out.writeLong(ChronicleTcp.ACTION_QUERY);
                    out.writeLong(queryIndex);
                    queryRequired = false;
                }
            }

            private void processIncoming(Bytes in) {
                if (in.remaining() >= ChronicleTcp.HEADER_SIZE) {
                    int size = in.readInt();
                    in.readLong(); // index

                    switch (size) {
                        case ChronicleTcp.IN_SYNC_LEN:
                            // heartbeat
                            state = TcpHandlerState.EXCERPT_NOT_FOUND;
                            return;
                        case ChronicleTcp.PADDED_LEN:
                            state = TcpHandlerState.EXCERPT_NOT_FOUND;
                            return;
                        case ChronicleTcp.SYNC_IDX_LEN:
                            // in sync
                            state = TcpHandlerState.EXCERPT_COMPLETE;
                            return;
                    }

                    state = TcpHandlerState.EXCERPT_NOT_FOUND;
                }
            }

            @Override
            public void onEndOfConnection(SessionDetailsProvider sessionDetailsProvider) {

            }

            protected void query(long index) throws IOException {
                queryIndex = index;
                queryRequired = true;
            }

        }
    }

    private final class StatefulExcerpt extends AbstractStatefulExcerpt {

        private AppenderAdapter adapter;

        private long lastLocalIndex;

        private StatefulExcerptTcpHandler tcpHandler = new StatefulExcerptTcpHandler();

        public StatefulExcerpt(final ExcerptCommon common) {
            super(common);

            this.adapter = null;
            this.lastLocalIndex = -1;
            this.withMapping(builder.withMapping());
            ChronicleQueueSink.this.sinkTcp.setSinkTcpHandler(tcpHandler);
            ChronicleQueueSink.this.sinkTcp.addConnectionListener(new TcpConnectionHandler() {
                @Override
                public void onConnect(SocketChannel channel) {
                    try {
                        bytesIn.clear();
                        bytesIn.limit(0);

                        if (adapter == null) {
                            adapter = createAdapter(wrappedChronicle);
                        }

                        tcpHandler.subscribeTo(lastLocalIndex = wrappedChronicle.lastIndex(), withMapping());

                        sinkTcp.write();
                    } catch (IOException ioe) {
                        builder.connectionListener().onError(sinkTcp.socketChannel(), ioe);
                    }
                }
            });
        }

        private boolean excerptNotRead(boolean busy, int attempts) {
            return (!busy && (attempts < readSpinCount || readSpinCount == -1));
        }

        @Override
        protected boolean doReadNextExcerpt() throws IOException {
            int attempts = 0;
            boolean busy;
            do {
                busy = sinkTcp.read();
            } while (excerptNotRead(busy, attempts++) || excerptIncomplete());

            return tcpHandler.state == TcpHandlerState.EXCERPT_COMPLETE;
        }

        private boolean excerptIncomplete() {
            return tcpHandler.state == TcpHandlerState.EXCERPT_INCOMPLETE;
        }

        @Override
        public void close() {
            if (this.adapter != null) {
                this.adapter.close();
                this.adapter = null;
            }

            super.close();
        }

        private class StatefulExcerptTcpHandler implements TcpHandler {

            private final BusyChecker busyChecker = new BusyChecker();

            private boolean subscriptionRequired;

            private long subscribedIndex;

            private MappingFunction mappingFunction;

            private TcpHandlerState state;

            private boolean subscribed;

            @Override
            public boolean process(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
                busyChecker.mark(in, out);
                processIncoming(in);
                processOutgoing(out);
                return busyChecker.busy(in, out);
            }

            private void processIncoming(Bytes in) {
                if (!subscribed) {
                    processSubscription(in);
                } else {
                    processExcerpt(in);
                }
            }

            private void processSubscription(Bytes in) {
                if (in.remaining() >= ChronicleTcp.HEADER_SIZE) {
                    long startPos = in.position();
                    int receivedSize = in.readInt();
                    in.readLong(); // index

                    switch (receivedSize) {
                        case ChronicleTcp.SYNC_IDX_LEN:
                            subscribed = true;
                            processExcerpt(in);
                            return;
                        case ChronicleTcp.IN_SYNC_LEN:
                            // heartbeat
                            return;
                        case ChronicleTcp.PADDED_LEN:
                            // padding
                            return;
                    }

                    // skip excerpt
                    if (in.remaining() >= receivedSize) {
                        in.skip(receivedSize);
                        state = TcpHandlerState.SEARCHING;
                    } else {
                        in.position(startPos);
                    }
                }
                subscribed = false;
            }

            private void processExcerpt(Bytes in) {
                if (state == TcpHandlerState.EXCERPT_INCOMPLETE) {
                    appendToExcerpt(in);
                } else if (in.remaining() >= ChronicleTcp.HEADER_SIZE) {
                    int receivedSize = in.readInt();
                    long receivedIndex = in.readLong();

                    switch (receivedSize) {
                        case ChronicleTcp.IN_SYNC_LEN:
                            // heartbeat
                            state = TcpHandlerState.EXCERPT_NOT_FOUND;
                            return;
                        case ChronicleTcp.PADDED_LEN:
                            // write padded entry
                            StatefulExcerpt.this.adapter.writePaddedEntry();
                            processExcerpt(in);
                            return;
                        case ChronicleTcp.SYNC_IDX_LEN:
                            //Sync IDX message, re-try
                            processExcerpt(in);
                            return;
                    }

                    if (receivedSize > 128 << 20 || receivedSize < 0) {
                        throw new TcpHandlingException("size was " + receivedSize);
                    }

                    if (lastLocalIndex != receivedIndex) {
                        StatefulExcerpt.this.adapter.startExcerpt(receivedSize, receivedIndex);

                        appendToExcerpt(in);
                    } else {
                        // skip the excerpt as we already have it
                        in.skip(receivedSize);
                        processExcerpt(in);
                    }

                } else {
                    state = TcpHandlerState.EXCERPT_NOT_FOUND;
                }

            }

            private void appendToExcerpt(Bytes in) {
                long inLimit = in.limit();
                int bytesToWrite = (int) Math.min(in.remaining(), adapter.remaining());

                try {
                    in.limit(in.position() + bytesToWrite);
                    adapter.write(in);

                    // needs more than one read.
                    if (adapter.remaining() > 0) {
                        state = TcpHandlerState.EXCERPT_INCOMPLETE;
                    } else {
                        state = TcpHandlerState.EXCERPT_COMPLETE;
                        adapter.finish();
                    }
                } finally {
                    // reset the limit;
                    in.limit(inLimit);
                }
            }

            private void processOutgoing(Bytes out) {
                if (subscriptionRequired) {
                    out.writeLong(ChronicleTcp.ACTION_SUBSCRIBE);
                    out.writeLong(subscribedIndex);

                    if (mappingFunction != null) {
                        // write with mapping and len
                        out.writeLong(ChronicleTcp.ACTION_WITH_MAPPING);
                        long pos = out.position();

                        out.skip(4);
                        long start = out.position();

                        out.writeObject(mappingFunction);
                        out.writeInt(pos, (int) (out.position() - start));
                    }

                    subscriptionRequired = false;
                }
            }

            @Override
            public void onEndOfConnection(SessionDetailsProvider sessionDetailsProvider) {

            }

            public void subscribeTo(long index, MappingFunction mappingFunction) {
                this.subscribedIndex = index;
                this.subscriptionRequired = true;
                this.mappingFunction = mappingFunction;
            }

        }

    }

    public enum TcpHandlerState {
        EXCERPT_INCOMPLETE,
        EXCERPT_COMPLETE,
        SEARCHING,
        EXCERPT_NOT_FOUND
    }
}
