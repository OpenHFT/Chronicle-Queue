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
 *
 */
package net.openhft.chronicle;

import net.openhft.chronicle.tcp.ChronicleTcp;
import net.openhft.chronicle.tcp.SinkTcp;
import net.openhft.chronicle.tools.WrappedChronicle;
import net.openhft.chronicle.tools.WrappedExcerptAppenders;
import net.openhft.chronicle.tools.WrappedExcerpts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;

class RemoteChronicleQueue extends WrappedChronicle {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteChronicleQueue.class);

    private final SinkTcp connection;
    private final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;
    private final boolean blocking;
    private volatile boolean closed;
    private long lastReconnectionAttemptMS;
    private long reconnectionIntervalMS;
    private long lastReconnectionAttempt;
    private ExcerptCommon excerpt;

    protected RemoteChronicleQueue(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, final SinkTcp connection, boolean blocking) {
        super(builder.chronicle());
        this.connection = connection;
        this.builder = builder.clone();
        this.closed = false;
        this.blocking = blocking;
        this.excerpt = null;
        this.reconnectionIntervalMS = builder.reconnectionIntervalMillis();
        this.lastReconnectionAttemptMS = 0;
        this.lastReconnectionAttempt = 0;
    }

    @Override
    public void close() throws IOException {
        if(!closed) {
            closed = true;
            closeConnection();
        }

        super.close();
    }

    @Override
    public Excerpt createExcerpt() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExcerptTailer createTailer() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExcerptAppender createAppender() throws IOException {
        throw new UnsupportedOperationException();
    }

    protected synchronized ExcerptCommon createAppender0() throws IOException {
        if( this.excerpt != null) {
            throw new IllegalStateException("An excerpt has already been created");
        }

        return this.excerpt = new StatelessExcerptAppender();
    }

    protected synchronized ExcerptCommon createExcerpt0() throws IOException {
        if( this.excerpt != null) {
            throw new IllegalStateException("An excerpt has already been created");
        }

        return this.excerpt = new StatelessExcerpt();
    }

    private boolean openConnection() {
        if(!connection.isOpen()) {
            try {
                connection.open(this.blocking);
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
                    LOGGER.warn("Failed to establish a connection {}",
                        ChronicleTcp.connectionName("", builder)
                    );
                }
            }
        }

        return connected;
    }

    private void closeConnection() {
        try {
            connection.close();
        } catch (IOException e) {
            LOGGER.warn("Error closing socketChannel", e);
        }
    }

    @Override
    public String name() {
        return null;
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

    // *************************************************************************
    // STATELESS
    // *************************************************************************

    private final class StatelessExcerptAppender
            extends WrappedExcerptAppenders.ByteBufferBytesExcerptAppenderWrapper {

        private final Logger logger;
        private final ByteBuffer readBuffer;
        private final ByteBuffer commandBuffer;
        private long lastIndex;
        private long actionType;

        public StatelessExcerptAppender() {
            super(builder.minBufferSize());

            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + connection.toString());
            this.readBuffer = ChronicleTcp.createBufferOfSize(12);
            this.commandBuffer = ChronicleTcp.createBufferOfSize(16);
            this.lastIndex = -1;
            this.actionType = builder.appendRequireAck() ? ChronicleTcp.ACTION_SUBMIT : ChronicleTcp.ACTION_SUBMIT_NOACK;
        }

        @Override
        public long capacity() {
            return super.limit();
        }

        @Override
        public void startExcerpt() {
            startExcerpt(builder.minBufferSize());
        }

        @Override
        public void startExcerpt(long excerptSize) {
            if(!isFinished()) {
                finish();
            }

            super.startExcerpt(excerptSize);
        }

        @Override
        public Chronicle chronicle() {
            return RemoteChronicleQueue.this;
        }

        @Override
        public void finish() {
            if(!isFinished()) {
                if(!connection.isOpen()) {
                    if(!waitForConnection()) {
                        super.finish();
                        throw new IllegalStateException("Unable to connect to the Source");
                    }
                }

                try {
                    connection.writeAction(commandBuffer, actionType, position());
                    ByteBuffer buffer = wrapped.buffer();
                    buffer.limit((int) wrapped.position());
                    connection.writeAllOrEOF(buffer);

                    if(builder.appendRequireAck()) {
                        connection.readUpTo(readBuffer, ChronicleTcp.HEADER_SIZE, -1);

                        int  recType  = readBuffer.getInt();
                        long recIndex = readBuffer.getLong();

                        switch(recType) {
                            case ChronicleTcp.ACK_LEN:
                                this.lastIndex = recIndex;
                                break;

                            case ChronicleTcp.NACK_LEN:
                                throw new IllegalStateException(
                                    "Message discarded by server, reason: " + (
                                        recIndex == ChronicleTcp.IDX_NOT_SUPPORTED
                                            ? "unsupported"
                                            : "unknown")
                                );
                            default:
                                logger.warn("Unknown message received {}, {}", recType, recIndex);
                        }
                    }
                } catch(IOException e) {
                    LOGGER.trace("", e);
                    throw new IllegalStateException(e);
                }
            }

            super.finish();
        }

        @Override
        public synchronized void close() {
            closeConnection();

            super.close();
            RemoteChronicleQueue.this.excerpt = null;
        }

        @Override
        public long index() {
            return -1;
        }

        @Override
        public long lastWrittenIndex() {
            return this.lastIndex;
        }

        private boolean waitForConnection() {
            for(int i=builder.reconnectionAttempts(); !connection.isOpen() && i>0; i--) {
                openConnection();

                if(!connection.isOpen()) {
                    try {
                        Thread.sleep(builder.reconnectionIntervalMillis());
                    } catch(InterruptedException ignored) {
                    }
                }
            }

            return connection.isOpen();
        }
    }

    private final class StatelessExcerpt
            extends WrappedExcerpts.ByteBufferBytesExcerptWrapper {

        private final Logger logger;
        private final ByteBuffer writeBuffer;
        private long index;
        private int readCount;

        public StatelessExcerpt() {
            super(builder.minBufferSize());

            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + connection.toString());
            this.index = -1;
            this.writeBuffer = ChronicleTcp.createBufferOfSize(16);
            this.readCount = builder.readSpinCount();
        }

        @Override
        public long index() {
            return index;
        }

        @Override
        public Excerpt toStart() {
            index(ChronicleTcp.IDX_TO_START);
            return this;
        }

        @Override
        public Excerpt toEnd() {
            index(ChronicleTcp.IDX_TO_END);
            return this;
        }

        @Override
        public Chronicle chronicle() {
            return RemoteChronicleQueue.this;
        }

        @Override
        public synchronized void close() {
            try {
                connection.writeAction(this.writeBuffer, ChronicleTcp.ACTION_UNSUBSCRIBE, ChronicleTcp.IDX_NONE);

                closeConnection();
            } catch (IOException e) {
                logger.warn("", e);
            }

            super.close();
            RemoteChronicleQueue.this.excerpt = null;
        }

        @Override
        public boolean index(long index) {
            try {
                if(!connection.isOpen()) {
                    if(shouldConnect()) {
                        if(!openConnection()) {
                            return false;
                        }

                        cleanup();

                    } else {
                        return false;
                    }
                }

                connection.writeAction(this.writeBuffer, ChronicleTcp.ACTION_SUBSCRIBE, index);

                while (true) {
                    connection.readUpTo(buffer(), ChronicleTcp.HEADER_SIZE, -1);

                    int  receivedSize  = buffer().getInt();
                    long receivedIndex = buffer().getLong();

                    switch(receivedSize) {
                        case ChronicleTcp.SYNC_IDX_LEN:
                            if (index == ChronicleTcp.IDX_TO_START) {
                                return receivedIndex == -1;

                            } else if (index == ChronicleTcp.IDX_TO_END) {
                                return advanceIndex();

                            } else if (index == receivedIndex) {
                                return advanceIndex();
                            }

                        case ChronicleTcp.IN_SYNC_LEN:
                        case ChronicleTcp.PADDED_LEN:
                            return false;
                    }

                    // skip excerpt
                    if (receivedSize > 0) {
                        connection.readUpTo(buffer(), receivedSize, -1);
                    }
                }
            } catch (IOException e) {
                if (e instanceof EOFException) {
                    logger.trace("", e);

                } else {
                    logger.warn("", e);
                }
            }

            return false;
        }

        @Override
        public boolean nextIndex() {
            finish();

            try {
                if(!connection.isOpen()) {
                    if (index(this.index)) {
                        return nextIndex();

                    } else {
                        return false;
                    }
                }

                if(!connection.readUpTo(buffer(), ChronicleTcp.HEADER_SIZE, this.readCount)) {
                    return false;
                }

                int  receivedSize  = buffer().getInt();
                long receivedIndex = buffer().getLong();

                switch (receivedSize) {
                    case ChronicleTcp.IN_SYNC_LEN:
                        return false;
                    case ChronicleTcp.SYNC_IDX_LEN:
                    case ChronicleTcp.PADDED_LEN:
                        return nextIndex();
                }

                if (receivedSize > 128 << 20 || receivedSize < 0) {
                    throw new StreamCorruptedException("Size was " + receivedSize);
                }

                resize(receivedSize);
                connection.readUpTo(buffer(), receivedSize, -1);

                index = receivedIndex;
            } catch (IOException e1) {
                if (e1 instanceof EOFException) {
                    logger.trace("Exception reading from socket", e1);

                } else {
                    logger.warn("Exception reading from socket", e1);
                }

                try {
                    connection.close();
                } catch (IOException e2) {
                    logger.warn("Error closing soocket", e2);
                }

                return false;
            }

            return true;
        }

        protected boolean advanceIndex() throws IOException {
            if(nextIndex()) {
                finish();
                return true;

            } else {
                return false;
            }
        }
    }
}
