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
package net.openhft.chronicle.tcp;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptCommon;
import net.openhft.chronicle.ExcerptComparator;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.tools.WrappedChronicle;
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

    private class AbstractStatelessExcerp extends NativeBytes {
        protected final Logger logger;

        protected AbstractStatelessExcerp() {
            super(new VanillaBytesMarshallerFactory(), NO_PAGE, NO_PAGE, null);

            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + connection.toString());
        }
    }

    private final class StatelessExcerpAppender extends AbstractStatelessExcerp implements ExcerptAppender {
        private ByteBuffer readBuffer;
        private ByteBuffer writeBuffer;
        private long lastIndex;
        private long actionType;

        public StatelessExcerpAppender() {
            super();

            int minSize = ChronicleTcp.nextPower2(builder.minBufferSize());

            this.writeBuffer = ChronicleTcp.createBufferOfSize(16 + minSize);
            this.readBuffer  = ChronicleTcp.createBufferOfSize(12);
            this.finished    = true;
            this.lastIndex   = -1;
            this.actionType  = builder.appendRequireAck() ? ChronicleTcp.ACTION_SUBMIT : ChronicleTcp.ACTION_SUBMIT_NOACK;
        }

        @Override
        public void startExcerpt() {
            startExcerpt(builder.minBufferSize());
        }

        @Override
        public void startExcerpt(long capacity) {
            if(!finished) {
                finish();
            }

            if(capacity <= this.capacity()) {
                this.positionAddr = this.startAddr + 16;
                this.limitAddr    = this.startAddr + 16 + capacity;
            } else {
                if(writeBuffer != null) {
                    ChronicleTcp.clean(writeBuffer);
                }

                int minSize = ChronicleTcp.nextPower2(builder.minBufferSize());

                this.writeBuffer  = ChronicleTcp.createBufferOfSize(16 + minSize);
                this.startAddr    = ChronicleTcp.address(this.writeBuffer);
                this.positionAddr = this.startAddr + 16;
                this.capacityAddr = this.startAddr + 16 + minSize;
                this.limitAddr    = this.startAddr + 16 + capacity;
            }

            // move limit and position at the expected size, buffer will be filled
            // through NativeBytes methods
            writeBuffer.limit(16 + (int)capacity);
            writeBuffer.position(16 + (int)capacity);

            finished = false;
        }

        @Override
        public ExcerptAppender toEnd() {
            return this;
        }

        @Override
        public Chronicle chronicle() {
            return RemoteChronicleQueue.this;
        }

        @Override
        public void finish() {
            if(!finished) {
                if(!connection.isOpen()) {
                    openConnection();
                }

                writeLong(0, this.actionType);
                writeLong(8, position() - 16);

                writeBuffer.flip();

                try {
                    connection.writeAllOrEOF(writeBuffer);

                    if(builder.appendRequireAck()) {
                        this.readBuffer.clear();
                        this.readBuffer.limit(ChronicleTcp.HEADER_SIZE);
                        this.readBuffer.flip();

                        if (connection.read(this.readBuffer, ChronicleTcp.HEADER_SIZE)) {
                            int  recType  = this.readBuffer.getInt();
                            long recIndex = this.readBuffer.getLong();

                            if (recType == ChronicleTcp.ACK_LEN) {
                                this.lastIndex = recIndex;
                            } else {
                                logger.warn("Unknown message received {}, {}", recType, recIndex);
                            }
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
        public boolean wasPadding() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long index() {
            return -1;
        }

        @Override
        public long lastWrittenIndex() {
            return this.lastIndex;
        }

        @Override
        public void addPaddedEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean nextSynchronous() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void nextSynchronous(boolean nextSynchronous) {
            throw new UnsupportedOperationException();
        }
    }

    private final class StatelessExcerpt extends AbstractStatelessExcerp implements Excerpt {
        private final ByteBuffer writeBuffer;
        private final ByteBuffer readBuffer;

        private long index;
        private int lastSize;

        public StatelessExcerpt() {
            super();

            this.index        = -1;
            this.lastSize     = 0;
            this.writeBuffer  = ChronicleTcp.createBufferOfSize(16);
            this.readBuffer   = ChronicleTcp.createBuffer(builder.minBufferSize());
            this.startAddr    = ChronicleTcp.address(this.readBuffer);
            this.capacityAddr = this.startAddr + builder.minBufferSize();
            this.limitAddr    = this.startAddr;
            this.finished     = true;
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
                writeBuffer.clear();
                writeBuffer.putLong(ChronicleTcp.ACTION_UNSUBSCRIBE);
                writeBuffer.putLong(ChronicleTcp.IDX_NONE);
                writeBuffer.flip();

                connection.writeAllOrEOF(writeBuffer);

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

                writeBuffer.clear();
                writeBuffer.putLong(ChronicleTcp.ACTION_SUBSCRIBE);
                writeBuffer.putLong(index);
                writeBuffer.flip();

                connection.writeAllOrEOF(writeBuffer);

                while (connection.read(readBuffer, ChronicleTcp.HEADER_SIZE)) {
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

                    try {
                        if ((receivedSize > 0) && (readBuffer.remaining() >= receivedSize)) {
                            readBuffer.position(readBuffer.position() + receivedSize);
                        }
                    } catch (IllegalArgumentException ex) {
                        logger.warn("index={}, position={}, limit={}, remaining={}, receivedSize={}",
                            index,
                            readBuffer.position(),
                            readBuffer.limit(),
                            readBuffer.remaining(),
                            receivedSize);

                        throw ex;
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

                if(!connection.read(this.readBuffer, ChronicleTcp.HEADER_SIZE, ChronicleTcp.HEADER_SIZE + 8)) {
                    return false;
                }

                int excerptSize = this.readBuffer.getInt();
                long receivedIndex = this.readBuffer.getLong();

                switch (excerptSize) {
                    case ChronicleTcp.IN_SYNC_LEN:
                        return false;
                    case ChronicleTcp.SYNC_IDX_LEN:
                    case ChronicleTcp.PADDED_LEN:
                        return nextIndex();
                }

                if (excerptSize > 128 << 20 || excerptSize < 0) {
                    throw new StreamCorruptedException("Size was " + excerptSize);
                }

                if(this.readBuffer.remaining() < excerptSize) {
                    if(!connection.read(this.readBuffer, excerptSize)) {
                        return false;
                    }
                }

                index        = receivedIndex;
                positionAddr = startAddr + this.readBuffer.position();
                limitAddr    = positionAddr + excerptSize;
                capacityAddr = limitAddr;
                lastSize     = excerptSize;
                finished     = false;
            } catch (IOException e) {
                close();
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

        @Override
        public long findMatch(@NotNull ExcerptComparator comparator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void findRange(@NotNull long[] startEnd, @NotNull ExcerptComparator comparator) {
            throw new UnsupportedOperationException();
        }
    }
}
