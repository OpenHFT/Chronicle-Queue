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

import net.openhft.chronicle.tcp.AppenderAdapter;
import net.openhft.chronicle.tcp.ChronicleTcp;
import net.openhft.chronicle.tcp.SinkTcp;
import net.openhft.chronicle.tools.ResizableDirectByteBufferBytes;
import net.openhft.chronicle.tools.WrappedChronicle;
import net.openhft.chronicle.tools.WrappedExcerpt;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;

import static net.openhft.chronicle.tcp.AppenderAdapters.createAdapter;
import static net.openhft.chronicle.tools.ChronicleTools.logIOException;

class ChronicleQueueSink extends WrappedChronicle {
    private final SinkTcp connection;
    private final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;
    private final boolean isLocal;
    private final int readSpinCount;
    private volatile boolean closed;
    private ExcerptCommon excerpt;

    ChronicleQueueSink(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, final SinkTcp connection) {
        super(builder.chronicle());
        this.connection = connection;
        this.builder = builder.clone();
        this.closed = false;
        this.isLocal = builder.sharedChronicle() && connection.isLocalhost();
        this.excerpt = null;
        this.readSpinCount = builder.readSpinCount();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            if (this.connection != null) {
                this.connection.close();
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
        protected final Bytes readBuffer;
        private long lastReconnectionAttemptMS;
        private long reconnectionIntervalMS;
        private long lastReconnectionAttempt;

        protected AbstractStatefulExcerpt(final ExcerptCommon excerpt) {
            super(excerpt);

            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + connection.toString());
            this.writeBuffer = new ResizableDirectByteBufferBytes(builder.minBufferSize());
            this.readBuffer = ByteBufferBytes.wrap(ChronicleTcp.createBuffer(builder.minBufferSize()));
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
                connection.close();
            } catch (IOException e) {
                logger.warn("Error closing socketChannel", e);
            }

            super.close();
            ChronicleQueueSink.this.excerpt = null;
        }

        protected boolean openConnection() {
            if(!connection.isOpen()) {
                try {
                    connection.open();
                } catch (IOException e) {
                }
            }

            boolean connected = connection.isOpen();
            if(connected) {
                builder.connectionListener().onConnect(connection.socketChannel());
                this.lastReconnectionAttempt = 0;
                this.lastReconnectionAttemptMS = 0;

            } else {
                lastReconnectionAttempt++;
                if(builder.reconnectionWarningThreshold() > 0) {
                    if (lastReconnectionAttempt > builder.reconnectionWarningThreshold()) {
                        logger.warn("Failed to establish a connection {}",
                                ChronicleTcp.connectionName("", builder)
                        );
                    }
                }
            }

            return connected;
        }

        protected boolean shouldConnect() {
            if(lastReconnectionAttempt >= builder.reconnectionAttempts()) {
                long now = System.currentTimeMillis();
                if (now < lastReconnectionAttemptMS + reconnectionIntervalMS) {
                    return false;
                }

                lastReconnectionAttemptMS = now;
            }

            return true;
        }

        protected void subscribe(long index) throws IOException {
            writeBuffer.clearAll();
            writeBuffer.writeLong(ChronicleTcp.ACTION_SUBSCRIBE);
            writeBuffer.writeLong(index);

            MappingFunction mapping = withMapping();
            if (mapping != null) {
                // write with mapping and len
                writeBuffer.writeLong(ChronicleTcp.ACTION_WITH_MAPPING);
                long pos = writeBuffer.position();

                writeBuffer.skip(4);
                long start = writeBuffer.position();

                writeBuffer.writeObject(mapping);
                writeBuffer.writeInt(pos, (int) (writeBuffer.position() - start));
            }

            writeBuffer.setBufferPositionAndLimit(0, writeBuffer.position());

            connection.writeAllOrEOF(writeBuffer);
        }

        protected void query(long index) throws IOException {
            writeBuffer.clearAll();
            writeBuffer.writeLong(ChronicleTcp.ACTION_QUERY);
            writeBuffer.writeLong(index);
            writeBuffer.setBufferPositionAndLimit(0, writeBuffer.position());

            connection.writeAllOrEOF(writeBuffer);
        }

        protected boolean readNext() {
            if (!closed && !connection.isOpen() && shouldConnect()) {
                try {
                    doReadNext();
                } catch(IOException e) {
                    logIOException(logger, "Exception reading from socket", e);
                    if(!closed) {
                        builder.connectionListener().onError(connection.socketChannel(), e);
                    }
                }
            }

            return !closed && connection.isOpen() && readNextExcerpt();
        }

        protected boolean readNextExcerpt() {
            try {
                if (!closed) {
                    return doReadNextExcerpt();
                }
            } catch (IOException e) {
                logIOException(logger, "Exception reading from socket", e);
                if(!closed) {
                    builder.connectionListener().onError(connection.socketChannel(), e);
                }

                try {
                    connection.close();
                    builder.connectionListener().onDisconnect(connection.socketChannel());
                } catch (IOException e2) {
                    logger.warn("Error closing socketChannel", e2);
                }
            }

            return false;
        }

        protected abstract boolean doReadNext()  throws IOException;
        protected abstract boolean doReadNextExcerpt() throws IOException;
    }

    // *************************************************************************
    // STATEFUL
    // *************************************************************************

    private class StatefulLocalExcerpt extends AbstractStatefulExcerpt {
        public StatefulLocalExcerpt(final ExcerptCommon common) {
            super(common);
        }

        @Override
        protected boolean doReadNext() throws IOException {
            if(openConnection()) {
                readBuffer.clear();
                readBuffer.limit(0);

                return true;
            }

            return false;
        }

        @Override
        protected boolean doReadNextExcerpt()  throws IOException {
            query(wrappedChronicle.lastIndex());

            if (connection.readUpTo(readBuffer, ChronicleTcp.HEADER_SIZE, readSpinCount)) {
                final int size = readBuffer.readInt();
                readBuffer.readLong(); // consume data

                switch (size) {
                    case ChronicleTcp.IN_SYNC_LEN:
                        return false;
                    case ChronicleTcp.PADDED_LEN:
                        return false;
                    case ChronicleTcp.SYNC_IDX_LEN:
                        return true;
                }
            }

            return false;
        }
    }

    private final class StatefulExcerpt extends AbstractStatefulExcerpt {
        private AppenderAdapter adapter;
        private long lastLocalIndex;

        public StatefulExcerpt(final ExcerptCommon common) {
            super(common);

            this.adapter = null;
            this.lastLocalIndex = -1;
            this.withMapping(builder.withMapping());
        }

        @Override
        protected boolean doReadNext() throws IOException {
            if(openConnection()) {
                readBuffer.clear();
                readBuffer.limit(0);

                if (this.adapter == null) {
                    this.adapter = createAdapter(wrappedChronicle);
                }

                subscribe(lastLocalIndex = wrappedChronicle.lastIndex());
                return true;
            }

            return false;
        }

        @Override
        protected boolean doReadNextExcerpt() throws IOException {
            if (!readAtLeastHeader()) {
                return false;
            }

            final int size = readBuffer.readInt();
            final long scIndex = readBuffer.readLong();

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
                long limit = readBuffer.limit();
                int size2 = (int) Math.min(readBuffer.remaining(), remaining);

                remaining -= size2;
                readBuffer.limit(readBuffer.position() + size2);
                adapter.write(readBuffer);
                // reset the limit;
                readBuffer.limit(limit);

                // needs more than one read.
                while (remaining > 0) {
                    int size3 = (int) Math.min(readBuffer.capacity(), remaining);
                    connection.readUpTo(readBuffer, size3, -1);
                    remaining -= readBuffer.remaining();
                    adapter.write(readBuffer);
                }

                adapter.finish();

            } else {
                readBuffer.position(readBuffer.position() + size);
                return readNextExcerpt();
            }

            return true;
        }

        private boolean readAtLeastHeader() throws IOException {
            long rem = readBuffer.remaining();
            if (rem < ChronicleTcp.HEADER_SIZE) {
                if (readBuffer.remaining() == 0) {
                    readBuffer.clear();
                } else {
                    compact(readBuffer);
                }
                System.out.println("About to read header");
                return connection.read(
                        readBuffer,
                        ChronicleTcp.HEADER_SIZE + 8,
                        readSpinCount);
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
    }
}
