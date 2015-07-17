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

import net.openhft.chronicle.network.SessionDetailsProvider;
import net.openhft.chronicle.network.TcpHandler;
import net.openhft.chronicle.network.TcpHandlingException;
import net.openhft.chronicle.tcp.*;
import net.openhft.chronicle.tools.ResizableDirectByteBufferBytes;
import net.openhft.chronicle.tools.WrappedChronicle;
import net.openhft.chronicle.tools.WrappedExcerpt;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.channels.SocketChannel;

import static net.openhft.chronicle.network.TcpPipeline.pipeline;
import static net.openhft.chronicle.tcp.AppenderAdapters.createAdapter;
import static net.openhft.chronicle.tools.ChronicleTools.logIOException;

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
        private long lastReconnectionAttemptMS;
        private long reconnectionIntervalMS;
        private long lastReconnectionAttempt;

        protected AbstractStatefulExcerpt(final ExcerptCommon excerpt) {
            super(excerpt);

            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + sinkTcp.toString());
            this.writeBuffer = new ResizableDirectByteBufferBytes(builder.minBufferSize());
            this.bytesIn = ByteBufferBytes.wrap(ChronicleTcp.createBuffer(builder.minBufferSize()));
            this.reconnectionIntervalMS = builder.reconnectionIntervalMillis();
            this.lastReconnectionAttemptMS = 0;
            this.lastReconnectionAttempt = 0;
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

        protected boolean shouldConnect() {
            if (lastReconnectionAttempt >= builder.reconnectionAttempts()) {
                long now = System.currentTimeMillis();
                if (now < lastReconnectionAttemptMS + reconnectionIntervalMS) {
                    return false;
                }

                lastReconnectionAttemptMS = now;
            }

            return true;
        }

/*
        protected void subscribe(long index) throws IOException {
            queueSinkTcpHandler.subscribeTo(index, withMapping());
            sinkTcp.sink();
        }
*/

        protected void query(long index) throws IOException {
            writeBuffer.clearAll();
            writeBuffer.writeLong(ChronicleTcp.ACTION_QUERY);
            writeBuffer.writeLong(index);

            // todo
//            sinkTcp.write(writeBuffer.flip());

            if (writeBuffer.remaining() > 0) {
                throw new EOFException("Failed to write query for index " + index);
            }
        }

        protected boolean readNext() {
/*
            if (!closed && !sinkTcp.connect(false)) {
                builder.connectionListener().onError(sinkTcp.socketChannel(), null);
            }

            if (!closed && !sinkTcp.isOpen() && shouldConnect()) {
                try {
                    doReadNext();
                } catch (IOException e) {
                    logIOException(logger, "Exception reading from socket", e);
                    if (!closed) {
                        builder.connectionListener().onError(sinkTcp.socketChannel(), e);
                    }
                }
            }
*/

            return !closed && sinkTcp.connect(false) && readNextExcerpt();
        }

        protected boolean readNextExcerpt() {
            try {
                if (!closed) {
                    return doReadNextExcerpt();
                }
            } catch (IOException e) {
                logIOException(logger, "Exception reading from socket", e);
                if (!closed) {
                    builder.connectionListener().onError(sinkTcp.socketChannel(), e);
                }

                try {
                    sinkTcp.close();
                    builder.connectionListener().onDisconnect(sinkTcp.socketChannel());
                } catch (IOException e2) {
                    logger.warn("Error closing socketChannel", e2);
                }
            }

            return false;
        }

//        protected abstract boolean doReadNext() throws IOException;

        protected abstract boolean doReadNextExcerpt() throws IOException;
    }

    // *************************************************************************
    // STATEFUL
    // *************************************************************************

    private class StatefulLocalExcerpt extends AbstractStatefulExcerpt {
        public StatefulLocalExcerpt(final ExcerptCommon common) {
            super(common);
            sinkTcp.addConnectionListener(new TcpConnectionHandler(){
                @Override
                public void onConnect(SocketChannel channel) {
                    bytesIn.clear();
                    bytesIn.limit(0);
                }
            });
        }

/*
        @Override
        protected boolean doReadNext() throws IOException {
            if (sinkTcp.connect(false)) {
                bytesIn.clear();
                bytesIn.limit(0);

                return true;
            }

            return false;
        }
*/

        @Override
        protected boolean doReadNextExcerpt() throws IOException {
            query(wrappedChronicle.lastIndex());

            // todo
/*
            if (sinkTcp.read(bytesIn.clear().limit(ChronicleTcp.HEADER_SIZE), ChronicleTcp.HEADER_SIZE, readSpinCount)) {
                final int size = bytesIn.readInt();
                bytesIn.readLong(); // consume data

                switch (size) {
                    case ChronicleTcp.IN_SYNC_LEN:
                        return false;
                    case ChronicleTcp.PADDED_LEN:
                        return false;
                    case ChronicleTcp.SYNC_IDX_LEN:
                        return true;
                }
            }
*/

            return false;
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
            ChronicleQueueSink.this.sinkTcp.setSinkTcpHandler(pipeline(tcpHandler));
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
                        sinkTcp.sink();
                    } catch (IOException ioe) {
                        builder.connectionListener().onError(sinkTcp.socketChannel(), ioe);
                    }
                }
            });
        }

/*
        @Override
        protected boolean doReadNext() throws IOException {
*/
/*
            if (sinkTcp.connect(false)) {
                bytesIn.clear();
                bytesIn.limit(0);

                if (this.adapter == null) {
                    this.adapter = createAdapter(wrappedChronicle);
                }

                subscribe(lastLocalIndex = wrappedChronicle.lastIndex());
                return true;
            }

*//*

            return false;
        }
*/

        private boolean headerNotFound(int attempts) {
            return (tcpHandler.state == TcpHandlerState.EXCERPT_NOT_FOUND && (attempts < readSpinCount && readSpinCount > -1));
        }

        @Override
        protected boolean doReadNextExcerpt() throws IOException {
            int attempts = 0;
            do {
                sinkTcp.sink();
            } while (headerNotFound(attempts++) || excerptIncomplete());

            return tcpHandler.state == TcpHandlerState.EXCERPT_COMPLETE;

/*
            if (!readAtLeastHeader()) {
                return false;
            }

            final int size = bytesIn.readInt();
            final long scIndex = bytesIn.readLong();

            switch (size) {
                case ChronicleTcp.IN_SYNC_LEN:
                    //Heartbeat message ignore and return false
                    return false;
                case ChronicleTcp.PADDED_LEN:
                    this.adapter.writePaddedEntry();
                    return readNextExcerpt();
                case ChronicleTcp.SYNC_IDX_LEN:
                    //Sync IDX message, re-try
                    return readNextExcerpt();
            }

            if (size > 128 << 20 || size < 0) {
                throw new StreamCorruptedException("size was " + size);
            }

            if (lastLocalIndex != scIndex) {
                this.adapter.startExcerpt(size, scIndex);

                long remaining = size;
                long limit = bytesIn.limit();
                int size2 = (int) Math.min(bytesIn.remaining(), remaining);

                remaining -= size2;
                bytesIn.limit(bytesIn.position() + size2);
                adapter.write(bytesIn);
                // reset the limit;
                bytesIn.limit(limit);

                // needs more than one read.
                while (remaining > 0) {
                    int size3 = (int) Math.min(bytesIn.capacity(), remaining);
                    // todo
//                    sinkTcp.read(bytesIn.clear().limit(size3), size3, -1);
                    remaining -= bytesIn.remaining();
                    adapter.write(bytesIn);
                }

                adapter.finish();

            } else {
                bytesIn.position(bytesIn.position() + size);
                return readNextExcerpt();
            }

            return true;
*/
        }

        private boolean excerptIncomplete() {
            return tcpHandler.state == TcpHandlerState.EXCERPT_INCOMPLETE;
        }

        private boolean readAtLeastHeader() throws IOException {
            long rem = bytesIn.remaining();
            if (rem < ChronicleTcp.HEADER_SIZE) {
                if (bytesIn.remaining() == 0) {
                    bytesIn.clear();
                } else {
                    compact(bytesIn);
                }
                // todo
                return true;
/*
                return sinkTcp.read(
                        bytesIn,
                        ChronicleTcp.HEADER_SIZE + 8,
                        readSpinCount);
*/
            }

            return true;
        }

        private void compact(Bytes bytes) {
            long pos = bytes.position();
            long rem = bytes.remaining();
            bytes.clear();
            bytes.write(bytes, pos, rem);
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

            private boolean subscriptionRequired;

            private long subscribedIndex;

            private MappingFunction mappingFunction;

            private TcpHandlerState state;

            @Override
            public void process(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
                processIncoming(in);
                processOutgoing(out);
            }

            private void processIncoming(Bytes in) {
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
                            processIncoming(in);
                            return;
                        case ChronicleTcp.SYNC_IDX_LEN:
                            //Sync IDX message, re-try
                            processIncoming(in);
                            return;
                    }

                    if (receivedSize > 128 << 20 || receivedSize < 0) {
                        throw new TcpHandlingException("size was " + receivedSize);
                    }

                    if (lastLocalIndex != receivedIndex) {
                        StatefulExcerpt.this.adapter.startExcerpt(receivedSize, receivedIndex);

                        appendToExcerpt(in);
/*
                        while (remaining > 0) {
                            int size3 = (int) Math.min(bytesIn.capacity(), remaining);
                            // todo
//                    sinkTcp.read(bytesIn.clear().limit(size3), size3, -1);
                            remaining -= bytesIn.remaining();
                            adapter.write(bytesIn);
                        }
*/

//                        adapter.finish();

                    } else {
                        // skip the excerpt as we already have it
                        in.skip(receivedSize);
//                        in.position(in.position() + receivedSize);
                        processIncoming(in);
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
            public void onEndOfConnection() {

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
        EXCERPT_NOT_FOUND
    }
}
