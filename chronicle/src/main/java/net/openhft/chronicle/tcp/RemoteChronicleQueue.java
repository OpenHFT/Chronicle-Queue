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
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.IOTools;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.io.serialization.impl.VanillaBytesMarshallerFactory;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;

public class RemoteChronicleQueue extends WrappedChronicle {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChronicleSink.class);

    private final SinkTcp connection;
    private final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;
    private final boolean isLocal;
    private volatile boolean closed;
    private ExcerptCommon excerpt;

    public RemoteChronicleQueue(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, final SinkTcp connection) {
        super(builder.chronicle());
        this.connection = connection;
        this.builder = builder.clone();
        this.closed = false;
        this.isLocal = builder.sharedChronicle() && connection.isLocalhost();
        this.excerpt = null;
    }

    @Override
    public void close() throws IOException {
        if(!closed) {
            closed = true;
            if (this.connection != null) {
                this.connection.close();
            }
        }

        super.close();
    }

    @Override
    public Excerpt createExcerpt() throws IOException {
        return (Excerpt)createExcerpt0();
    }

    @Override
    public synchronized ExcerptTailer createTailer() throws IOException {
        return (ExcerptTailer)createExcerpt0();
    }

    @Override
    public ExcerptAppender createAppender() throws IOException {
        throw new UnsupportedOperationException();
    }

    private ExcerptCommon createExcerpt0() throws IOException {
        if( this.excerpt != null) {
            throw new IllegalStateException("An excerpt has already been created");
        }

        return this.excerpt = new StatelessExcerpt();
    }

    // *************************************************************************
    // STATELESS
    // *************************************************************************

    private final class StatelessAppender extends NativeBytes implements ExcerptAppender {
        private final Logger logger;
        private final ByteBuffer readBuffer;
        private ByteBuffer writeBuffer;

        private long index;

        public StatelessAppender() {
            super(new VanillaBytesMarshallerFactory(), NO_PAGE, NO_PAGE, null);

            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + connection.toString());
            this.writeBuffer = ChronicleTcp.createBufferOfSize(builder.minBufferSize());
            this.readBuffer = ChronicleTcp.createBuffer(12);
            this.startAddr = ((DirectBuffer) this.readBuffer).address();
            this.capacityAddr = this.startAddr + builder.minBufferSize();
            this.limitAddr = this.capacityAddr;
            this.finished = true;
        }

        @Override
        public void startExcerpt() {
            startExcerpt(builder.minBufferSize());
        }

        @Override
        public void startExcerpt(long capacity) {
            if(capacity <= this.capacity()) {
                writeBuffer.clear();
            } else {
                IOTools.clean(writeBuffer);
                this.writeBuffer = ChronicleTcp.createBufferOfSize((int)capacity);
            }
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

            }

            super.finish();
        }

        @Override
        public synchronized void close() {
            super.close();
            RemoteChronicleQueue.this.excerpt = null;
        }

        @Override
        public boolean wasPadding() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long index() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long lastWrittenIndex() {
            throw new UnsupportedOperationException();
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

    private final class StatelessExcerpt extends NativeBytes implements Excerpt {
        private final Logger logger;
        private final ByteBuffer writeBuffer;
        private final ByteBuffer readBuffer;

        private long index;
        private int lastSize;

        public StatelessExcerpt() {
            super(new VanillaBytesMarshallerFactory(), NO_PAGE, NO_PAGE, null);

            this.index = -1;
            this.lastSize = 0;
            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + connection.toString());
            this.writeBuffer = ChronicleTcp.createBufferOfSize(16);
            this.readBuffer = ChronicleTcp.createBuffer(builder.minBufferSize());
            this.startAddr = ((DirectBuffer) this.readBuffer).address();
            this.capacityAddr = this.startAddr + builder.minBufferSize();
            this.finished = true;
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
        public long lastWrittenIndex() {
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
            this.index = index;
            this.lastSize = 0;

            try {
                if(!connection.isOpen()) {
                    connection.open();
                    readBuffer.clear();
                    readBuffer.limit(0);
                }

                writeBuffer.clear();
                writeBuffer.putLong(ChronicleTcp.ACTION_SUBSCRIBE);
                writeBuffer.putLong(this.index);
                writeBuffer.flip();

                connection.writeAllOrEOF(writeBuffer);

                while (connection.read(readBuffer, ChronicleTcp.HEADER_SIZE)) {
                    int receivedSize = readBuffer.getInt();
                    long receivedIndex = readBuffer.getLong();

                    switch(receivedSize) {
                        case ChronicleTcp.SYNC_IDX_LEN:
                            if(index == ChronicleTcp.IDX_TO_START) {
                                return receivedIndex == -1;
                            } else if(index == ChronicleTcp.IDX_TO_END) {
                                return advanceIndex();
                            } else {
                                return (index == receivedIndex) ? advanceIndex() : false;
                            }
                        case ChronicleTcp.PADDED_LEN:
                        case ChronicleTcp.IN_SYNC_LEN:
                            return false;
                    }

                    if (readBuffer.remaining() >= receivedSize) {
                        readBuffer.position(readBuffer.position() + receivedSize);
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

                index = receivedIndex;
                positionAddr = startAddr + this.readBuffer.position();
                limitAddr = positionAddr + excerptSize;
                capacityAddr = limitAddr;
                lastSize = excerptSize;
                finished = false;
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
