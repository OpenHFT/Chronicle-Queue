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
import net.openhft.lang.Maths;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.io.serialization.impl.VanillaBytesMarshallerFactory;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;

class RemoteChronicleQueue extends WrappedChronicle {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteChronicleQueue.class);

    private final SinkTcp connection;
    private final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;
    private final boolean blocking;
    private volatile boolean closed;
    private ExcerptCommon excerpt;

    protected RemoteChronicleQueue(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, final SinkTcp connection, boolean blocking) {
        super(builder.chronicle());
        this.connection = connection;
        this.builder = builder.clone();
        this.closed = false;
        this.blocking = blocking;
        this.excerpt = null;
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

        return this.excerpt = new StatelessExcerpAppender();
    }

    protected synchronized ExcerptCommon createExcerpt0() throws IOException {
        if( this.excerpt != null) {
            throw new IllegalStateException("An excerpt has already been created");
        }

        return this.excerpt = new StatelessExcerpt();
    }

    private void openConnection() {
        for(int i=0; !connection.isOpen(); i++) {
            try {
                connection.open(this.blocking);
            } catch (IOException e) {
                if(i > 10) {
                    try {
                        Thread.sleep(builder.reconnectTimeoutMillis());
                    } catch (InterruptedException ex) {
                    }

                    LOGGER.warn("", e);
                }
            }
        }
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

    // *************************************************************************
    // STATELESS
    // *************************************************************************

    private final class StatelessExcerpAppender
            extends WrappedExcerptAppenders.ByteBufferBytesExcerptAppender {

        private final Logger logger;
        private final ByteBuffer readBuffer;
        private final ByteBuffer commandBuffer;
        private long lastIndex;
        private long actionType;

        public StatelessExcerpAppender() {
            super(builder.minBufferSize());

            this.logger        = LoggerFactory.getLogger(getClass().getName() + "@" + connection.toString());
            this.readBuffer    = ChronicleTcp.createBufferOfSize(12);
            this.commandBuffer = ChronicleTcp.createBufferOfSize(16);
            this.lastIndex     = -1;
            this.actionType    = builder.appendRequireAck() ? ChronicleTcp.ACTION_SUBMIT : ChronicleTcp.ACTION_SUBMIT_NOACK;
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
                    openConnection();
                }

                try {
                    connection.writeAction(commandBuffer, actionType, position());
                    connection.writeAllOrEOF(wrappedAppender.buffer());

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
                    LOGGER.warn("", e);
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
    }

    private final class StatelessExcerpt extends NativeBytes implements Excerpt {
        private final Logger logger;

        private ByteBuffer writeBuffer;
        private ByteBuffer readBuffer;

        private long index;
        private int lastSize;
        private int readCount;

        public StatelessExcerpt() {
            super(new VanillaBytesMarshallerFactory(), NO_PAGE, NO_PAGE, null);

            this.logger       = LoggerFactory.getLogger(getClass().getName() + "@" + connection.toString());
            this.index        = -1;
            this.lastSize     = 0;
            this.writeBuffer  = ChronicleTcp.createBufferOfSize(16);
            this.readBuffer   = ChronicleTcp.createBuffer(builder.minBufferSize());
            this.startAddr    = ChronicleTcp.address(this.readBuffer);
            this.capacityAddr = this.startAddr + builder.minBufferSize();
            this.limitAddr    = this.startAddr;
            this.finished     = true;
            this.readCount    = builder.readSpinCount();
        }

        @Override
        public boolean wasPadding() {
            return false;
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
        public void finish() {
            if(!isFinished()) {
                if (lastSize > 0) {
                    readBuffer.position(readBuffer.position() + lastSize);
                }

                super.finish();
            }
        }

        @Override
        public boolean index(long index) {
            this.lastSize = 0;

            try {
                if(!connection.isOpen()) {
                    openConnection();

                    readBuffer.clear();
                    readBuffer.limit(0);
                }

                connection.writeAction(this.writeBuffer, ChronicleTcp.ACTION_SUBSCRIBE, index);

                while (true) {
                    connection.readUpTo(this.readBuffer, ChronicleTcp.HEADER_SIZE, -1);

                    int  receivedSize  = readBuffer.getInt();
                    long receivedIndex = readBuffer.getLong();

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
                        connection.readUpTo(this.readBuffer, receivedSize, -1);
                    }
                }
            } catch (IOException e) {
                logger.warn("", e);
            }

            return false;
        }

        @Override
        public boolean nextIndex() {
            finish();

            try {
                if(!connection.isOpen()) {
                    if(index(this.index)) {
                        return nextIndex();
                    } else {
                        return false;
                    }
                }

                if(!connection.readUpTo(this.readBuffer, ChronicleTcp.HEADER_SIZE, this.readCount)) {
                    return false;
                }

                int  receivedSize  = this.readBuffer.getInt();
                long receivedIndex = this.readBuffer.getLong();

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

                if(receivedSize > this.readBuffer.capacity()) {
                    ChronicleTcp.clean(this.readBuffer);

                    this.readBuffer   = ChronicleTcp.createBuffer(Maths.nextPower2(receivedSize, receivedSize));
                    this.startAddr    = ChronicleTcp.address(this.readBuffer);
                    this.capacityAddr = this.startAddr + builder.minBufferSize();
                    this.limitAddr    = this.startAddr;
                }

                connection.readUpTo(this.readBuffer, receivedSize, -1);

                index        = receivedIndex;
                positionAddr = startAddr;
                limitAddr    = positionAddr + receivedSize;
                capacityAddr = limitAddr;
                lastSize     = receivedSize;
                finished     = false;
            } catch (IOException e) {
                close();
                return false;
            }

            return true;
        }

        @Override
        public long findMatch(@NotNull ExcerptComparator comparator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void findRange(@NotNull long[] startEnd, @NotNull ExcerptComparator comparator) {
            throw new UnsupportedOperationException();
        }

        protected boolean advanceIndex() throws IOException {
            if(nextIndex()) {
                finish();
                return true;
            } else {
                return false;
            }
        }

        /*
        protected void ensureReadBufferCapacity(int size) {
            if(!ChronicleTcp.hasCapacityOf(this.readBuffer, size)) {
                ChronicleTcp.clean(this.readBuffer);

                int bufferSize = Maths.nextPower2(builder.minBufferSize(), size);

                this.readBuffer   = ChronicleTcp.createBufferOfSize(bufferSize);
                this.startAddr    = ChronicleTcp.address(this.readBuffer);
                this.capacityAddr = this.startAddr + bufferSize;
                this.limitAddr    = this.startAddr;
            }

            positionAddr = startAddr;
            limitAddr    = startAddr + size;
            capacityAddr = limitAddr;
        }
        */
    }
}
