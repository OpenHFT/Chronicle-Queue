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
import net.openhft.chronicle.tcp.SinkTcpRemoteAppenderHandler;
import net.openhft.chronicle.tcp.SinkTcpRemoteExcerptHandler;
import net.openhft.chronicle.tools.ResizableDirectByteBufferBytes;
import net.openhft.chronicle.tools.WrappedChronicle;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.WrappedBytes;
import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.thread.LightPauser;
import net.openhft.lang.thread.Pauser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.tcp.ChronicleTcp.createBufferOfSize;
import static net.openhft.lang.io.ByteBufferBytes.wrap;

class RemoteChronicleQueue extends WrappedChronicle {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteChronicleQueue.class);

    private final SinkTcp sinkTcp;
    private final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;
    private final boolean blocking;
    private volatile boolean closed;
    private ExcerptCommon excerpt;

    protected RemoteChronicleQueue(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, final SinkTcp sinkTcp, boolean blocking) {
        super(builder.chronicle());
        this.sinkTcp = sinkTcp;
        this.builder = builder.clone();
        this.closed = false;
        this.blocking = blocking;
        this.excerpt = null;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
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
        if (this.excerpt != null) {
            throw new IllegalStateException("An excerpt has already been created");
        }

        return this.excerpt = new StatelessExcerptAppender();
    }

    protected synchronized ExcerptCommon createExcerpt0() throws IOException {
        if (this.excerpt != null) {
            throw new IllegalStateException("An excerpt has already been created");
        }

        return this.excerpt = new StatelessExcerpt();
    }

    private void closeConnection() {
        try {
            sinkTcp.close();
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

    private final class StatelessExcerptAppender
            extends AbstractStatelessExcerpt implements ExcerptAppender {

        private final Logger logger;
        private final Bytes readBuffer;
        private final ByteBuffer commandBuffer;
        private long lastIndex;
        private long actionType;
        private SinkTcpRemoteAppenderHandler sinkTcpHandler;

        private final Pauser pauser = new LightPauser(TimeUnit.MILLISECONDS.toNanos(1L), TimeUnit.MILLISECONDS.toNanos(10L));

        public StatelessExcerptAppender() {
            super(wrap(createBufferOfSize(builder.minBufferSize())));

            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + sinkTcp.toString());
            this.readBuffer = wrap(createBufferOfSize(12));
            this.commandBuffer = createBufferOfSize(16);
            this.lastIndex = -1;
            this.actionType = builder.appendRequireAck() ? ChronicleTcp.ACTION_SUBMIT : ChronicleTcp.ACTION_SUBMIT_NOACK;
            this.sinkTcpHandler = new SinkTcpRemoteAppenderHandler(this);
            this.sinkTcpHandler.setAppendRequireAck(builder.appendRequireAck());
            RemoteChronicleQueue.this.sinkTcp.setSinkTcpHandler(sinkTcpHandler);
        }

        @Override
        public void startExcerpt() {
            startExcerpt(builder.minBufferSize());
        }

        @Override
        public void startExcerpt(long size) {
            if (!isFinished()) {
                finish();
            }

            if (size <= capacity()) {
                clear();
                limit(size);
            } else {
                wrapped = wrap(createBufferOfSize((int) size));
            }

        }

        @Override
        public void addPaddedEntry() {

        }

        @Override
        public boolean nextSynchronous() {
            return false;
        }

        @Override
        public void nextSynchronous(boolean nextSynchronous) {

        }

        @Override
        public Chronicle chronicle() {
            return RemoteChronicleQueue.this;
        }

        @Override
        public void finish() {
            if (!isFinished()) {
                if (!sinkTcp.isOpen()) {
                    if (!waitForConnection()) {
                        super.finish();
                        throw new IllegalStateException("Unable to connect to the Source");
                    }
                }

                try {
                    flip();

                    sinkTcp.sink();
                    pauser.reset();
                    while (sinkTcpHandler.waitingForAck()) {
                        pauser.pause();
                        sinkTcp.sink();
                    }


/*
                    if (remaining() > 0) {
                        // only write anything if there is anything to write
                        sinkTcp.writeAction(commandBuffer, actionType, limit());
                        sinkTcp.write(this);

                        if (remaining() > 0) {
                            throw new EOFException("Failed to write content for index " + index());
                        }

                        if (builder.appendRequireAck()) {
                            sinkTcp.read(readBuffer.clear().limit(ChronicleTcp.HEADER_SIZE), ChronicleTcp.HEADER_SIZE, -1);

                            int recType = readBuffer.readInt();
                            long recIndex = readBuffer.readLong();

                            switch (recType) {
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
                    }
*/
                } catch (IOException e) {
                    LOGGER.trace("", e);
                    throw new IllegalStateException(e);
                }
            }

            super.finish();
        }

        @Override
        public boolean read8bitText(@NotNull StringBuilder stringBuilder) throws StreamCorruptedException {
            return false;
        }

        @Override
        public void write8bitText(CharSequence charSequence) {

        }

        @Override
        public synchronized void close() {
            closeConnection();

            super.close();
            RemoteChronicleQueue.this.excerpt = null;
        }

        @Override
        public boolean wasPadding() {
            return false;
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
            for (int i = builder.reconnectionAttempts(); !sinkTcp.isOpen() && i > 0; i--) {
                sinkTcp.connect(blocking);

                if (!sinkTcp.isOpen()) {
                    try {
                        Thread.sleep(builder.reconnectionIntervalMillis());
                    } catch (InterruptedException ignored) {
                    }
                }
            }

            return sinkTcp.isOpen();
        }

        @Override
        public long findMatch(@NotNull ExcerptComparator comparator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void findRange(@NotNull long[] startEnd, @NotNull ExcerptComparator comparator) {

        }

        @Override
        public boolean index(long l) {
            return false;
        }

        @Override
        public boolean nextIndex() {
            return false;
        }

        @Override
        public Excerpt toStart() {
            return null;
        }

        @Override
        public Excerpt toEnd() {
            return null;
        }
    }

    private abstract class AbstractStatelessExcerpt extends WrappedBytes<Bytes> implements Excerpt {

        protected AbstractStatelessExcerpt(Bytes wrapped) {
            super(wrapped);
        }

        protected Bytes resize(long capacity) {
            if (capacity > Integer.MAX_VALUE) {
                throw new IllegalStateException("Only capacities up to Integer.MAX_VALUE are supported");
            }

            if (capacity > capacity()) {
                wrapped = wrap(createBufferOfSize((int) capacity));
            }

            clear();
            limit((int) capacity);
            return this;
        }

        protected void cleanup() {
            clear();
        }

    }

    private final class StatelessExcerpt
//            extends WrappedExcerpts.ByteBufferBytesExcerptWrapper {
            extends AbstractStatelessExcerpt implements SinkTcp.ConnectionListener {
        private final Logger logger;
        private final ByteBuffer writeBuffer;
        private long index;
        private int readCount;
        private SinkTcpRemoteExcerptHandler sinkTcpRemoteExcerptHandler = new SinkTcpRemoteExcerptHandler();
        private final Pauser pauser;

        public StatelessExcerpt() {
            super(new ResizableDirectByteBufferBytes(builder.minBufferSize()));
            this.pauser = new LightPauser(builder.busyPeriodTimeNanos(), builder.parkPeriodTimeNanos());
            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + sinkTcp.toString());
            this.index = -1;
            this.writeBuffer = createBufferOfSize(16);
            this.readCount = builder.readSpinCount();
            sinkTcp.setConnectionListener(this);
            sinkTcp.setSinkTcpHandler(sinkTcpRemoteExcerptHandler);
            sinkTcpRemoteExcerptHandler.setContent((ResizableDirectByteBufferBytes) super.wrapped);
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
        public boolean read8bitText(@NotNull StringBuilder stringBuilder) throws StreamCorruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write8bitText(CharSequence charSequence) {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized void close() {
            closeConnection();
/*
            try {
                sinkTcp.writeAction(this.writeBuffer, ChronicleTcp.ACTION_UNSUBSCRIBE, ChronicleTcp.IDX_NONE);

                closeConnection();
            } catch (IOException e) {
                logger.warn("", e);
            }
*/

            super.close();
            RemoteChronicleQueue.this.excerpt = null;
        }

        @Override
        public long findMatch(@NotNull ExcerptComparator comparator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void findRange(@NotNull long[] startEnd, @NotNull ExcerptComparator comparator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean index(long index) {
            try {
                if (!sinkTcp.connect(blocking)) {
                    return false;
                }

                sinkTcpRemoteExcerptHandler.subscribeTo(index);
                sinkTcp.sink();

//                sinkTcp.writeAction(this.writeBuffer, ChronicleTcp.ACTION_SUBSCRIBE, index);

                while (true) {
                    sinkTcp.sink();
                    switch (sinkTcpRemoteExcerptHandler.getState()) {
                        case INDEX_FOUND:
                            return true;
                        case INDEX_NOT_FOUND:
                            return false;
                    }
                    pauser.pause();

/*

                    sinkTcp.read(resize(ChronicleTcp.HEADER_SIZE), ChronicleTcp.HEADER_SIZE, -1);

                    int receivedSize = readInt();
                    long receivedIndex = readLong();

                    switch (receivedSize) {
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
                        sinkTcp.read(resize(receivedSize), receivedSize, -1);
                    }
*/
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
                if (!sinkTcp.isOpen()) {
                    return index(this.index) && nextIndex();
                }

                int attempts = 0;
                //noinspection LoopStatementThatDoesntLoop
                do {
                    sinkTcp.sink();
                    switch (sinkTcpRemoteExcerptHandler.getState()) {
                        case INDEX_NOT_FOUND:
                            return false;
                        case INDEX_FOUND:
                            index = sinkTcpRemoteExcerptHandler.index();
                            return true;
                    }
                    pauser.pause();
                } while (readCount > -1 && attempts++ < readCount);

                return false;
/*
                if (!sinkTcp.read(resize(ChronicleTcp.HEADER_SIZE), ChronicleTcp.HEADER_SIZE, this.readCount)) {
                    return false;
                }

                int receivedSize = readInt();
                long receivedIndex = readLong();

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

                sinkTcp.read(resize(receivedSize), receivedSize, -1);

                index = receivedIndex;
*/
            } catch (IOException e1) {
                if (e1 instanceof EOFException) {
                    logger.trace("Exception reading from socket", e1);

                } else {
                    logger.warn("Exception reading from socket", e1);
                }

                try {
                    sinkTcp.close();
                } catch (IOException e2) {
                    logger.warn("Error closing soocket", e2);
                }

                return false;
            }

//            return true;
        }

        protected boolean advanceIndex() throws IOException {
            if (nextIndex()) {
                finish();
                return true;

            } else {
                return false;
            }
        }

        @Override
        public void onConnect() {
            cleanup();
        }
    }
}
