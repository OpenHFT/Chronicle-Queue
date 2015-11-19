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
import net.openhft.chronicle.tcp.TcpConnectionHandler;
import net.openhft.chronicle.tcp.TcpHandlingException;
import net.openhft.chronicle.tcp.network.SessionDetailsProvider;
import net.openhft.chronicle.tcp.network.TcpHandler;
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
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.tcp.ChronicleTcp.createBufferOfSize;
import static net.openhft.lang.io.ByteBufferBytes.wrap;

class RemoteChronicleQueue extends WrappedChronicle {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteChronicleQueue.class);

    private final SinkTcp sinkTcp;
    private final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;
    private volatile boolean closed;
    private ExcerptCommon excerpt;

    protected RemoteChronicleQueue(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, final SinkTcp sinkTcp) {
        super(builder.chronicle());
        this.sinkTcp = sinkTcp;
        this.builder = builder.clone();
        this.closed = false;
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

    protected synchronized ExcerptCommon createAppender0() {
        if( this.excerpt != null) {
            throw new IllegalStateException("An excerpt has already been created");
        }

        return this.excerpt = new StatelessExcerptAppender();
    }

    protected synchronized ExcerptCommon createExcerpt0() {
        if( this.excerpt != null) {
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
        private long lastIndex;
        private StatelessExcerptAppenderTcpHandler sinkTcpHandler;

        private final Pauser pauser = new LightPauser(TimeUnit.MILLISECONDS.toNanos(1L), TimeUnit.MILLISECONDS.toNanos(10L));

        public StatelessExcerptAppender() {
            super(wrap(createBufferOfSize(builder.minBufferSize())));

            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + sinkTcp.toString());
            this.lastIndex = -1;
            this.sinkTcpHandler = new StatelessExcerptAppenderTcpHandler(this);
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

                    sinkTcpHandler.markUnsent();
                    // A pipeline attached to a sinkTcp may need to be called multiple
                    // times in order for the excerpt to actually be isSent.  This could
                    // be caused by a TcpHandler higher up in the pipeline.
                    do {
                        sinkTcp.write();
                    } while (!sinkTcpHandler.isSent());

                    while (sinkTcpHandler.waitingForAck()) {
                        pauser.pause();
                        sinkTcp.read();
                    }
                    pauser.reset();
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
                sinkTcp.connect();

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

        private class StatelessExcerptAppenderTcpHandler implements TcpHandler {

            private final Logger logger = LoggerFactory.getLogger(StatelessExcerptAppenderTcpHandler.class);

            private final BusyChecker busyChecker = new BusyChecker();

            private ExcerptAppender appender;

            private boolean appendRequireAck;

            private boolean waitingForAck;

            private boolean sent = false;

            public StatelessExcerptAppenderTcpHandler(ExcerptAppender appender) {
                this.appender = appender;
            }

            @Override
            public boolean process(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
                busyChecker.mark(in, out);
                processIncoming(in);
                processOutgoing(out);
                return busyChecker.busy(in, out);
            }

            private void processIncoming(Bytes in) {
                if (waitingForAck && in.remaining() >= ChronicleTcp.HEADER_SIZE) {

                    int recType = in.readInt();
                    long recIndex = in.readLong();

                    switch (recType) {
                        case ChronicleTcp.ACK_LEN:
                            StatelessExcerptAppender.this.lastIndex = recIndex;
                            waitingForAck = false;
                            break;

                        case ChronicleTcp.NACK_LEN:
                            waitingForAck = false;
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

            private void processOutgoing(Bytes out) {
                if (appender.remaining() > 0) {
                    // only write anything if there is anything to write
                    out.writeLong(appendRequireAck ? ChronicleTcp.ACTION_SUBMIT : ChronicleTcp.ACTION_SUBMIT_NOACK);
                    out.writeLong(appender.limit());
                    out.write(((StatelessExcerptAppender)appender).wrapped);

                    if (appender.remaining() > 0) {
                        throw new TcpHandlingException("Failed to write content for index " + appender.index());
                    }

                    waitingForAck = appendRequireAck;
                }
                sent = true;
            }

            @Override
            public void onEndOfConnection(SessionDetailsProvider sessionDetailsProvider) {

            }

            public void setAppendRequireAck(boolean appendRequireAck) {
                this.appendRequireAck = appendRequireAck;
            }

            public boolean waitingForAck() {
                return waitingForAck;
            }

            public long getLastIndex() {
                return lastIndex;
            }

            public void markUnsent() {
                sent = false;
            }

            public boolean isSent() {
                return sent;
            }
        }

    }

    private abstract class AbstractStatelessExcerpt extends WrappedBytes<Bytes> implements Excerpt {

        protected AbstractStatelessExcerpt(Bytes wrapped) {
            super(wrapped);
        }

        protected void cleanup() {
            clear();
        }

        @Override
        public void writeEnum(long offset, int maxSize, Object o) {
            wrapped.writeEnum(offset, maxSize, o);
        }

        @Override
        public <E> E readEnum(long offset, int maxSize, Class<E> aClass) {
            return wrapped.readEnum(offset, maxSize, aClass);
        }
    }

    private final class StatelessExcerpt
            extends AbstractStatelessExcerpt {
        private final Logger logger;
        private long index;
        private int readSpinCount;
        private RemoteExcerptTcpHandler tcpHandler = new RemoteExcerptTcpHandler();
        private final Pauser pauser;

        public StatelessExcerpt() {
            super(new ResizableDirectByteBufferBytes(builder.minBufferSize()));
            this.pauser = new LightPauser(builder.busyPeriodTimeNanos(), builder.parkPeriodTimeNanos());
            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + sinkTcp.toString());
            this.index = -1;
            this.readSpinCount = builder.readSpinCount();
            sinkTcp.addConnectionListener(new TcpConnectionHandler(){
                @Override
                public void onConnect(SocketChannel channel) {
                    cleanup();
                }
            });
            sinkTcp.setSinkTcpHandler(tcpHandler);
            tcpHandler.setContent((ResizableDirectByteBufferBytes) super.wrapped);
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
            try {
                tcpHandler.unsubscribe();
                sinkTcp.write();
                closeConnection();
            } catch (IOException e) {
                logger.warn("", e);
            }

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
                if (!sinkTcp.connect()) {
                    return false;
                }

                tcpHandler.subscribeTo(index);
                sinkTcp.write();

                while (true) {
                    switch (tcpHandler.getState()) {
                        case EXCERPT_COMPLETE:
                            this.index = tcpHandler.index();
                            return true;
                        case EXCERPT_NOT_FOUND:
                            return false;
                    }
                    pauser.pause();
                    sinkTcp.read();
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

        private boolean excerptNotRead(boolean busy, int attempts) {
            return (!busy && (attempts < readSpinCount || readSpinCount == -1));
        }

        @Override
        public boolean nextIndex() {
            finish();

            try {
                if (!sinkTcp.isOpen()) {
                    return index(this.index) && nextIndex();
                }

                pauser.reset();
                int attempts = 0;
                boolean busy;
                //noinspection LoopStatementThatDoesntLoop
                do {
                    busy = sinkTcp.read();
                    switch (tcpHandler.getState()) {
                        case EXCERPT_NOT_FOUND:
                            return false;
                        case EXCERPT_COMPLETE:
                            index = tcpHandler.index();
                            return true;
                    }
                    pauser.pause();
                } while (excerptNotRead(busy, attempts++) || tcpHandler.state == TcpHandlerState.EXCERPT_INCOMPLETE);

                return false;
            } catch (IOException e1) {
                if (e1 instanceof EOFException) {
                    logger.trace("Exception reading from socket", e1);

                } else {
                    logger.warn("Exception reading from socket", e1);
                }

                try {
                    sinkTcp.close();
                } catch (IOException e2) {
                    logger.warn("Error closing socket", e2);
                }

                return false;
            }

        }

        public class RemoteExcerptTcpHandler implements TcpHandler {

            private boolean subscribed;

            private boolean subscriptionRequired;

            private boolean unsubscribeRequired;

            private long subscribedIndex;

            private ResizableDirectByteBufferBytes content;

            private long index = -1;

            private TcpHandlerState state = TcpHandlerState.EXCERPT_INCOMPLETE;

            private final BusyChecker busyChecker = new BusyChecker();

            @Override
            public boolean process(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
                busyChecker.mark(in, out);
                processIncoming(in);
                processOutgoing(out);
                return busyChecker.busy(in, out);
            }

            private void processOutgoing(Bytes out) {
                if (subscriptionRequired) {
                    out.writeLong(ChronicleTcp.ACTION_SUBSCRIBE);
                    out.writeLong(subscribedIndex);
                    subscriptionRequired = false;
                } else if (unsubscribeRequired) {
                    out.writeLong(ChronicleTcp.ACTION_UNSUBSCRIBE);
                    out.writeLong(ChronicleTcp.IDX_NONE);
                    subscribed = false;
                    unsubscribeRequired = false;
                }
            }

            private void processIncoming(Bytes in) {
                if (!subscribed) {
                    processSubscription(in);
                } else {
                    nextExcerpt(in);
                }
            }

            private void processSubscription(Bytes in) {
                if (in.remaining() >= ChronicleTcp.HEADER_SIZE) {
                    long startPos = in.position();
                    int receivedSize = in.readInt();
                    long receivedIndex = in.readLong();

                    switch (receivedSize) {
                        case ChronicleTcp.SYNC_IDX_LEN:
                            if (subscribedIndex == ChronicleTcp.IDX_TO_START) {
                                state = receivedIndex == -1 ? TcpHandlerState.EXCERPT_COMPLETE : TcpHandlerState.EXCERPT_NOT_FOUND;
                                subscribed = true;
                                return;

                            } else if (subscribedIndex == ChronicleTcp.IDX_TO_END) {
                                nextExcerpt(in);
                                subscribed = true;
                                return;

                            } else if (subscribedIndex == receivedIndex) {
                                nextExcerpt(in);
                                subscribed = true;
                                return;
                            }

                        case ChronicleTcp.IN_SYNC_LEN:
                        case ChronicleTcp.PADDED_LEN:
                            state = TcpHandlerState.EXCERPT_NOT_FOUND;
                            subscribed = true;
                            return;
                    }

                    // skip excerpt
                    if (in.remaining() >= receivedSize) {
                        long inLimit = in.limit();
                        try {
                            in.limit(in.position() + receivedSize);
                            content.resetToSize(receivedSize).write(in);
                            content.flip();
                        } finally {
                            in.limit(inLimit);
                        }
                        state = TcpHandlerState.SEARCHING;
                    } else {
                        in.position(startPos);
                        state = TcpHandlerState.EXCERPT_INCOMPLETE;
                    }
                } else {
                    state = TcpHandlerState.EXCERPT_INCOMPLETE;
                }
                subscribed = false;
            }

            private void nextExcerpt(Bytes in) {
                if (in.remaining() >= ChronicleTcp.HEADER_SIZE) {
                    long startPos = in.position();
                    int receivedSize = in.readInt();
                    long receivedIndex = in.readLong();

                    switch (receivedSize) {
                        case ChronicleTcp.IN_SYNC_LEN:
                            // heartbeat
                            state = TcpHandlerState.EXCERPT_NOT_FOUND;
                            return;
                        case ChronicleTcp.SYNC_IDX_LEN:
                        case ChronicleTcp.PADDED_LEN:
                            nextExcerpt(in);
                            return;
                    }

                    if (receivedSize > 128 << 20 || receivedSize < 0) {
                        throw new TcpHandlingException("Size was " + receivedSize);
                    }

                    if (in.remaining() >= receivedSize) {
                        long inLimit = in.limit();
                        try {
                            in.limit(in.position() + receivedSize);
                            content.resetToSize(receivedSize).write(in);
                            content.flip();
                        } finally {
                            in.limit(inLimit);
                        }

                        index = receivedIndex;
                        state = TcpHandlerState.EXCERPT_COMPLETE;
                    } else {
                        in.position(startPos);
                        state = TcpHandlerState.EXCERPT_INCOMPLETE;
                    }
                } else {
                    state = TcpHandlerState.EXCERPT_INCOMPLETE;
                }
            }

            @Override
            public void onEndOfConnection(SessionDetailsProvider sessionDetailsProvider) {

            }

            public void subscribeTo(long index) {
                this.subscribedIndex = index;
                this.subscriptionRequired = true;
                this.subscribed = false;
            }

            public void setContent(ResizableDirectByteBufferBytes content) {
                this.content = content;
            }

            public TcpHandlerState getState() {
                return state;
            }

            public long index() {
                return index;
            }

            public void unsubscribe() {
                this.unsubscribeRequired = true;
            }
        }
    }

    public enum TcpHandlerState {
        EXCERPT_NOT_FOUND,
        EXCERPT_COMPLETE,
        SEARCHING,
        EXCERPT_INCOMPLETE
    }
}
