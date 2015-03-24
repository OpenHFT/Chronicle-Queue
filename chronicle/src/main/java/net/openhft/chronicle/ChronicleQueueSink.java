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

import net.openhft.chronicle.tcp.ChronicleTcp;
import net.openhft.chronicle.tcp.SinkTcp;
import net.openhft.chronicle.tools.WrappedChronicle;
import net.openhft.chronicle.tools.WrappedExcerpt;
import net.openhft.chronicle.tools.WrappedExcerptAppender;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;

class ChronicleQueueSink extends WrappedChronicle {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChronicleQueueSink.class);

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
    // STATEFUL
    // *************************************************************************

    private abstract class AbstractStatefulExcerpt extends WrappedExcerpt {
        protected final Logger logger;
        protected final ByteBuffer writeBuffer;
        protected final ByteBufferBytes writeBufferBytes;
        protected final ByteBuffer readBuffer;
        private long lastReconnectionAttemptMS;
        private long reconnectionIntervalMS;
        private long lastReconnectionAttempt;

        protected AbstractStatefulExcerpt(final ExcerptCommon excerpt) {
            super(excerpt);

            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + connection.toString());
            this.writeBuffer = ChronicleTcp.createBuffer(16);
            this.writeBufferBytes = new ByteBufferBytes(writeBuffer);
            this.readBuffer = ChronicleTcp.createBuffer(builder.minBufferSize());
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
            writeBuffer.clear();
            writeBufferBytes.clear();

            writeBufferBytes.writeLong(ChronicleTcp.ACTION_SUBSCRIBE);
            writeBufferBytes.writeLong(index);

            MappingFunction mapping = withMapping();
            if (mapping != null) {
                // write with mapping and len
                writeBufferBytes.writeLong(ChronicleTcp.ACTION_WITH_MAPPING);
                long pos = writeBufferBytes.position();

                writeBufferBytes.skip(4);
                long start = writeBufferBytes.position();

                writeBufferBytes.writeObject(mapping);
                int len = (int) (writeBufferBytes.position() - start);

                writeBufferBytes.writeInt(pos, len);

            }

            writeBuffer.position(0);
            writeBuffer.limit((int) writeBufferBytes.position());

            connection.writeAllOrEOF(writeBuffer);
        }

        protected void query(long index) throws IOException {
            writeBuffer.clear();
            writeBuffer.putLong(ChronicleTcp.ACTION_QUERY);
            writeBuffer.putLong(index);
            writeBuffer.flip();

            connection.writeAllOrEOF(writeBuffer);
        }

        protected abstract boolean readNext();
    }

    private class StatefulLocalExcerpt extends AbstractStatefulExcerpt {
        public StatefulLocalExcerpt(final ExcerptCommon common) {
            super(common);
        }

        @Override
        protected boolean readNext() {
            if (!closed && !connection.isOpen() && shouldConnect()) {
                if(openConnection()) {
                    readBuffer.clear();
                    readBuffer.limit(0);
                }
            }

            return !closed && connection.isOpen() && readNextExcerpt();
        }

        private boolean readNextExcerpt() {
            try {
                if (!closed) {
                    query(wrappedChronicle.lastIndex());

                    if (connection.readUpTo(readBuffer, ChronicleTcp.HEADER_SIZE, readSpinCount)) {
                        final int size = readBuffer.getInt();
                        // consume data
                        readBuffer.getLong();

                        switch (size) {
                            case ChronicleTcp.IN_SYNC_LEN:
                                return false;
                            case ChronicleTcp.PADDED_LEN:
                                //TODO: Indexed Vs Vanilla
                                return false;
                            case ChronicleTcp.SYNC_IDX_LEN:
                                return true;
                        }
                    }
                }
            } catch (IOException e1) {
                if (e1 instanceof EOFException) {
                    logger.debug("Error reading from socket", e1);
                } else {
                    logger.warn("Error reading from socket", e1);
                }

                try {
                    connection.close();
                } catch (IOException e2) {
                    logger.warn("Error closing socketChannel", e2);
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
        protected boolean readNext() {
            if (!closed && !connection.isOpen() && shouldConnect()) {
                if(openConnection()) {
                    readBuffer.clear();
                    readBuffer.limit(0);

                    try {
                        if (this.adapter == null) {
                            this.adapter = createAppenderAdapter();
                        }

                        subscribe(lastLocalIndex = wrappedChronicle.lastIndex());
                    } catch (IOException e) {
                        if (e instanceof EOFException) {
                            logger.debug("Error reading from socket", e);
                        } else {
                            logger.warn("Error reading from socket", e);
                        }

                        return false;
                    }
                }
            }

            return connection.isOpen() && readNextExcerpt();
        }

        private boolean readNextExcerpt() {
            try {
                if (!closed && !connection.read(
                        readBuffer,
                        ChronicleTcp.HEADER_SIZE,
                        ChronicleTcp.HEADER_SIZE + 8,
                        readSpinCount)) {
                    return false;
                }

                final int size = readBuffer.getInt();
                final long scIndex = readBuffer.getLong();

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
                    int limit = readBuffer.limit();
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
            } catch (IOException e1) {
                if (e1 instanceof EOFException) {
                    logger.trace("Exception reading from socket", e1);
                } else {
                    logger.warn("Exception reading from socket", e1);
                }

                try {
                    connection.close();
                } catch (IOException e2) {
                    logger.warn("Error closing socketChannel", e2);
                }
            }

            return true;
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

    // *************************************************************************
    // Appender adapters
    // *************************************************************************

    /**
     * Creates a SinkAppenderAdapter.
     *
     * @return the SinkAppenderAdapter
     * @throws java.io.IOException
     */
    private AppenderAdapter createAppenderAdapter() throws IOException {
        if (wrappedChronicle instanceof IndexedChronicle) {
            return new IndexedAppenderAdapter(wrappedChronicle, wrappedChronicle.createAppender());
        }

        if (wrappedChronicle instanceof VanillaChronicle) {
            return new VanillaAppenderAdapter(wrappedChronicle, wrappedChronicle.createAppender());
        }

        throw new IllegalArgumentException("Can only adapt Indexed or Vanilla chronicles");
    }

    private abstract class AppenderAdapter extends WrappedExcerptAppender<ExcerptAppender> {

        public AppenderAdapter(@NotNull ExcerptAppender appender) {
            super(appender);
        }

        public abstract void writePaddedEntry();

        public abstract void startExcerpt(long capacity, long index);
    }

    /**
     * IndexedChronicle AppenderAdapter
     */
    private final class IndexedAppenderAdapter extends AppenderAdapter {
        private final IndexedChronicle chronicle;

        public IndexedAppenderAdapter(@NotNull final Chronicle chronicle, @NotNull final ExcerptAppender appender) {
            super(appender);

            this.chronicle = (IndexedChronicle) chronicle;
        }

        @Override
        public void writePaddedEntry() {
            super.wrapped.addPaddedEntry();
        }

        @Override
        public void startExcerpt(long capacity, long index) {
            super.wrapped.startExcerpt(capacity);
        }
    }

    /**
     * VanillaChronicle AppenderAdapter
     */
    private final class VanillaAppenderAdapter extends AppenderAdapter {
        private final VanillaChronicle chronicle;
        private final VanillaChronicle.VanillaAppender appender;

        public VanillaAppenderAdapter(@NotNull final Chronicle chronicle, @NotNull final ExcerptAppender appender) {
            super(appender);

            this.chronicle = (VanillaChronicle) chronicle;
            this.appender = (VanillaChronicle.VanillaAppender) appender;
        }

        @Override
        public void writePaddedEntry() {
            LOGGER.warn("VanillaChronicle should not receive padded entries");
        }

        @Override
        public void startExcerpt(long capacity, long index) {
            int cycle = (int) (index >>> chronicle.getEntriesForCycleBits());
            this.appender.startExcerpt(capacity, cycle);
        }
    }
}
