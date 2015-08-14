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
package net.openhft.chronicle.tcp;

import net.openhft.chronicle.*;
import net.openhft.chronicle.tcp.network.*;
import net.openhft.chronicle.tools.ResizableDirectByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.thread.LightPauser;
import net.openhft.lang.thread.Pauser;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class SourceTcp {
    protected final Logger logger;
    protected final String name;
    protected final AtomicBoolean running;
    protected final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;
    protected final ThreadPoolExecutor executor;
    protected final LightPauser pauser;

    protected SourceTcp(String name, final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, ThreadPoolExecutor executor) {
        this.builder = builder;
        this.name = ChronicleTcp.connectionName(name, this.builder.bindAddress(), this.builder.connectAddress());
        this.logger = LoggerFactory.getLogger(this.name);
        this.running = new AtomicBoolean(false);
        this.executor = executor;
        this.pauser = new LightPauser(builder.busyPeriodTimeNanos(), builder.parkPeriodTimeNanos());
    }

    public SourceTcp open() {
        this.running.set(true);
        this.executor.execute(createHandler());

        return this;
    }

    public boolean close() {
        running.set(false);
        executor.shutdown();

        try {
            executor.awaitTermination(
                    builder.selectTimeout() * 2,
                    builder.selectTimeoutUnit());
        } catch (InterruptedException ignored) {
        }

        return !running.get();
    }

    @Override
    public String toString() {
        return this.name;
    }

    public void dataNotification() {
        pauser.unpause();
    }

    public abstract boolean isLocalhost();

    protected abstract Runnable createHandler();

    /**
     * Creates a session handler according to the Chronicle the sources is connected to.
     *
     * @param socketChannel The {@link java.nio.channels.SocketChannel}
     * @return The Runnable
     */
    protected Runnable createSessionHandler(final @NotNull SocketChannel socketChannel) throws IOException {
        final Chronicle chronicle = builder.chronicle();
        if (chronicle != null) {
            if (chronicle instanceof IndexedChronicle) {
                builder.connectionListener().onConnect(socketChannel);
                return new IndexedSessionHandler(socketChannel);

            } else if (chronicle instanceof VanillaChronicle) {
                builder.connectionListener().onConnect(socketChannel);
                return new VanillaSessionHandler(socketChannel);

            } else {
                throw new IllegalStateException("Chronicle must be Indexed or Vanilla");
            }
        }

        throw new IllegalStateException("Chronicle can't be null");
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * Abstract class for Indexed and Vanilla chronicle replication
     */
    private abstract class SessionHandler implements SourceTcpHandler.SubscriptionListener, Runnable, Closeable {
        private final SocketChannel socketChannel;
        protected final TcpEventHandler tcpEventHandler;
        private final SourceTcpHandler sourceTcpHandler;
        private final SimpleSessionDetailsProvider sessionDetails;

        protected ExcerptTailer tailer;
        protected ExcerptAppender appender;

        private SessionHandler(final @NotNull SocketChannel socketChannel, SourceTcpHandler sourceTcpHandler) throws IOException {
            this.socketChannel = socketChannel;
            this.sourceTcpHandler = sourceTcpHandler;
            this.sourceTcpHandler.setSubscriptionListener(this);
            this.sourceTcpHandler.setPauser(pauser);
            this.sourceTcpHandler.setMaxExcerptsPerMessage(builder.maxExcerptsPerMessage());
            this.sourceTcpHandler.setHeartbeatIntervalMillis(builder.heartbeatIntervalMillis());
            this.sessionDetails = new SimpleSessionDetailsProvider();
            this.tcpEventHandler = new TcpEventHandler(socketChannel, builder.tcpPipeline(sourceTcpHandler), sessionDetails, builder.connectionListener(), builder.sendBufferSize(), builder.receiveBufferSize());
            this.tailer = null;
            this.appender = null;
        }

        @Override
        public void subscribed() {
            sessionDetails.get(SelectionKey.class).interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        }

        @Override
        public void unSubscribed() {
            final SelectionKey selectionKey = sessionDetails.get(SelectionKey.class);
            selectionKey.interestOps(selectionKey.interestOps() & ~SelectionKey.OP_WRITE);
        }

        @Override
        public void close() throws IOException {
            if (this.tailer != null) {
                this.tailer.close();
                this.tailer = null;
            }

            if (this.appender != null) {
                this.appender.close();
                this.appender = null;
            }

            if (this.socketChannel != null) {
                this.socketChannel.close();
                try {
                    // trigger onEndOfConnection on the tcp handlers of the event handler.
                    tcpEventHandler.action();
                } catch (InvalidEventHandlerException iehe) {
                    // this is expected as the socket is closed.
                }
            }
        }

        @Override
        public void run() {
            VanillaSelectionKeySet selectionKeys = null;

            try {
                final VanillaSelector selector = new VanillaSelector()
                        .open()
                        .register(socketChannel, SelectionKey.OP_READ, new Attached());

                tailer = builder.chronicle().createTailer();
                appender = builder.chronicle().createAppender();

                sourceTcpHandler.setTailer(tailer);
                sourceTcpHandler.setAppender(appender);

                selectionKeys = selector.vanillaSelectionKeys();

                if (selectionKeys != null) {
                    vanillaNioLoop(selector, selectionKeys);
                } else {
                    nioLoop(selector);
                }
            } catch (InvalidEventHandlerException e) {
                if (running.get()) {
                    logger.info("Connection {} died", socketChannel);
                }
            } catch (Exception e) {
                if (running.get()) {
                    logger.info("Connection {} died", socketChannel, e);
                }
            } finally {
                if (selectionKeys != null) {
                    selectionKeys.clear();
                }
            }

            try {
                close();
            } catch (IOException e) {
                logger.warn("", e);
            }
        }

        private void vanillaNioLoop(final VanillaSelector selector, final VanillaSelectionKeySet selectionKeys) throws IOException, InvalidEventHandlerException {
            final int spinLoopCount = builder.selectorSpinLoopCount();
            final long selectTimeout = builder.selectTimeout();

            while (running.get()) {
                int nbKeys = selector.select(spinLoopCount, selectTimeout);

                if (nbKeys > 0) {
                    final SelectionKey[] keys = selectionKeys.keys();
                    final int size = selectionKeys.size();

                    for (int k = 0; k < size; k++) {
                        final SelectionKey key = keys[k];
                        if (key != null) {
                            if (!onSelectionKey(key)) {
                                break;
                            }
                        }
                    }

                    selectionKeys.clear();
                }
            }
        }

        private void nioLoop(final VanillaSelector selector) throws IOException, InvalidEventHandlerException {
            final int spinLoopCount = builder.selectorSpinLoopCount();
            final long selectTimeout = builder.selectTimeout();

            while (running.get()) {
                int nbKeys = selector.select(spinLoopCount, selectTimeout);

                if (nbKeys > 0) {
                    final Set<SelectionKey> keys = selector.selectionKeys();
                    for (final SelectionKey key : keys) {
                        if (!onSelectionKey(key)) {
                            break;
                        }
                    }

                    keys.clear();
                }
            }
        }

        protected boolean onSelectionKey(final SelectionKey key) throws InvalidEventHandlerException {
            if (key != null) {
                if (key.isReadable() || key.isWritable()) {
                    try {
                        sessionDetails.set(SelectionKey.class, key);
                        sessionDetails.set(MappingProvider.class, (MappingProvider) key.attachment());
                        return tcpEventHandler.action();
                    } finally {
                        sessionDetails.set(SelectionKey.class, null);
                    }
                }
            }

            return true;
        }

    }

    // *************************************************************************
    // SessionHandler - implementations
    // *************************************************************************

    /**
     * IndexedChronicle session handler
     */
    private class IndexedSessionHandler extends SessionHandler {

        private IndexedSessionHandler(final @NotNull SocketChannel socketChannel) throws IOException {
            super(socketChannel, SourceTcpHandler.indexed());
        }

    }

    /**
     * VanillaChronicle session handler
     */
    private class VanillaSessionHandler extends SessionHandler {

        private VanillaSessionHandler(final @NotNull SocketChannel socketChannel) throws IOException {
            super(socketChannel, SourceTcpHandler.vanilla());
        }

    }

    public static abstract class SourceTcpHandler implements TcpHandler {

        protected Pauser pauser;

        protected ExcerptTailer tailer;

        protected ExcerptAppender appender;

        protected SubscriptionListener subscriptionListener;

        private boolean subscribed;

        private long lastHeartbeat;

        private long heartbeatIntervalMillis;

        private final BusyChecker busyChecker = new BusyChecker();

        protected int maxExcerptsPerMessage;

        protected long index;

        protected ResizableDirectByteBufferBytes withMappedBuffer;

        protected TcpHandlerState state;

        protected Bytes content;

        public SourceTcpHandler() {
            this.withMappedBuffer = new ResizableDirectByteBufferBytes(1024);
        }

        public void setTailer(ExcerptTailer tailer) {
            this.tailer = tailer;
        }

        public void setPauser(Pauser pauser) {
            this.pauser = pauser;
        }

        public void setSubscriptionListener(SubscriptionListener subscriptionListener) {
            this.subscriptionListener = subscriptionListener;
        }

        public void setMaxExcerptsPerMessage(int maxExcerptsPerMessage) {
            this.maxExcerptsPerMessage = maxExcerptsPerMessage;
        }

        public void setHeartbeatIntervalMillis(long heartbeatIntervalMillis) {
            this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        }

        @Override
        public boolean process(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
            busyChecker.mark(in, out);
            processIncoming(in, out, sessionDetailsProvider);
            processOutgoing(out, sessionDetailsProvider);
            return busyChecker.busy(in, out);
        }

        private void processIncoming(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
            while (in.remaining() >= 8) {

                final long action = in.readLong();
                try {
                    switch ((int) action) {
                        case (int) ChronicleTcp.ACTION_WITH_MAPPING:
                            if (!onMapping(in, sessionDetailsProvider)) {
                                unreadAction(in);
                                return;
                            }
                            break;
                        case (int) ChronicleTcp.ACTION_SUBSCRIBE:
                            if (!(subscribed = onSubscribe(in, out, sessionDetailsProvider))) {
                                unreadAction(in); // wind back the long we read
                                return;
                            }
                            break;
                        case (int) ChronicleTcp.ACTION_UNSUBSCRIBE:
                            if (!onUnsubscribe(in)) {
                                unreadAction(in);
                                return;
                            }
                            break;
                        case (int) ChronicleTcp.ACTION_QUERY:
                            if (!onQuery(in, out)) {
                                in.position(in.position() - 8);
                                return;
                            }
                            break;
                        case (int) ChronicleTcp.ACTION_SUBMIT:
                            if (!onSubmit(in, out, true)) {
                                in.position(in.position() - 8);
                                return;
                            }
                            break;
                        case (int) ChronicleTcp.ACTION_SUBMIT_NOACK:
                            if (!onSubmit(in, out, false)) {
                                in.position(in.position() - 8);
                                return;
                            }
                            break;
                        default:
                            throw new IOException("Unknown action received (" + action + ")");
                    }
                } catch (IOException e) {
                    throw new TcpHandlingException(e);
                }
            }
        }

        private void unreadAction(Bytes in) {
            in.position(in.position() - 8);
        }

        private void processOutgoing(Bytes out, SessionDetailsProvider sessionDetailsProvider) {
            final long now = System.currentTimeMillis();

            if (subscribed && !write(out, sessionDetailsProvider)) {
                if (lastHeartbeat <= now && out.remaining() >= ChronicleTcp.HEADER_SIZE) {
                    writeSizeAndIndex(out, ChronicleTcp.IN_SYNC_LEN, ChronicleTcp.IDX_NONE);
                }
            }
        }

        protected abstract boolean write(Bytes out, SessionDetailsProvider sessionDetailsProvider);

        protected void writeSizeAndIndex(Bytes out, int size, long index) {
            out.writeInt(size);
            out.writeLong(index);
            setLastHeartbeat();
        }

        protected void setLastHeartbeat() {
            setLastHeartbeat(System.currentTimeMillis());
        }

        protected void setLastHeartbeat(long now) {
            this.lastHeartbeat = now + heartbeatIntervalMillis;
        }

        @Override
        public void onEndOfConnection(SessionDetailsProvider sessionDetailsProvider) {

        }

        protected abstract boolean onSubmit(Bytes in, Bytes out, boolean ack) throws IOException;

        protected abstract boolean onSubscribe(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider);

        protected boolean onUnsubscribe(Bytes in) throws IOException {
            if (in.remaining() >= 8) {
                in.readLong(); // do nothing with the long
                subscriptionListener.unSubscribed();
                return true;
            }
            return false;
        }

        protected boolean onQuery(Bytes in, Bytes out) throws IOException {
            if (in.remaining() >= 8) {
                long index = in.readLong();
                if (tailer.index(index)) {
                    final long now = System.currentTimeMillis();
                    setLastHeartbeat(now);

                    while (true) {
                        if (tailer.nextIndex()) {
                            writeSizeAndIndex(out, ChronicleTcp.SYNC_IDX_LEN, tailer.index());
                            tailer.finish();
                            break;

                        } else {
                            if (lastHeartbeat <= now) {
                                writeSizeAndIndex(out, ChronicleTcp.IN_SYNC_LEN, ChronicleTcp.IDX_NONE);
                                break;
                            }
                        }
                    }
                } else {
                    writeSizeAndIndex(out, ChronicleTcp.IN_SYNC_LEN, 0L);
                }
                return true;
            }

            return false;
        }

        protected boolean onMapping(Bytes in, SessionDetailsProvider sessionDetailsProvider) throws IOException {
            if (in.remaining() >= 4) {
                int size = in.readInt();
                if (in.remaining() >= size) {
                    MappingProvider mappingProvider = sessionDetailsProvider.get(MappingProvider.class);
                    if (mappingProvider != null) {
                        MappingFunction mappingFunction = in.readObject(MappingFunction.class);
                        mappingProvider.withMapping(mappingFunction);
                    }

                    return true;
                } else {
                    in.position(in.position() - 4);
                }
            }
            return false;
        }

        /**
         * applies a mapping if the mapping is not set to {@code}null{code}
         *
         * @param source          the tailer for the mapping to be applied to
         * @param mappingProvider the key attachment
         * @return returns the tailer or the mapped bytes
         * @see
         */
        protected Bytes applyMapping(@NotNull final ExcerptTailer source,
                                     @Nullable MappingProvider mappingProvider) {
            if (mappingProvider == null) {
                return source;
            }

            final MappingFunction mappingFunction = mappingProvider.withMapping();
            if (mappingFunction == null) {
                return source;
            }

            withMappedBuffer.clear();
            if (withMappedBuffer.capacity() < source.limit()) {
                withMappedBuffer.resetToSize((int) source.capacity());
            }

            try {
                mappingFunction.apply(source, withMappedBuffer);
            } catch (IllegalArgumentException e) {
                // lets try to resize
                if (e.getMessage().contains("Attempt to write")) {
                    if (withMappedBuffer.capacity() == Integer.MAX_VALUE) {
                        throw e;
                    }

                    withMappedBuffer.resetToSize(
                            Math.min(
                                    Integer.MAX_VALUE,
                                    (int) (withMappedBuffer.capacity() * 1.5))
                    );

                } else {
                    throw e;
                }
            }

            return withMappedBuffer.flip();
        }

        protected void writePartial(Bytes out) {
            int bytesToWrite = (int) Math.min(content.remaining(), out.remaining());
            long contentLimit = content.limit();
            out.write(content.limit(content.position() + bytesToWrite));
            content.limit(contentLimit);

            state = content.remaining() > 0 ? TcpHandlerState.EXCERPT_INCOMPLETE : TcpHandlerState.EXCERPT_COMPLETE;

        }

        protected boolean hasRoomForExcerpt(Bytes bytes, Bytes tailer) {
            return hasRoomFor(bytes, tailer.remaining() + ChronicleTcp.HEADER_SIZE);
        }

        protected boolean hasRoomFor(Bytes bytes, long size) {
            return bytes.remaining() >= size;
        }

        public void setAppender(ExcerptAppender appender) {
            this.appender = appender;
        }

        public static VanillaSourceTcpHandler vanilla() {
            return new VanillaSourceTcpHandler();
        }

        public static class VanillaSourceTcpHandler extends SourceTcpHandler {

            private boolean nextIndex;

            @Override
            protected boolean write(Bytes out, SessionDetailsProvider sessionDetailsProvider) {
                if (state == TcpHandlerState.EXCERPT_INCOMPLETE) {
                    writePartial(out);
                    if (state == TcpHandlerState.EXCERPT_INCOMPLETE || out.remaining() <= ChronicleTcp.HEADER_SIZE) {
                        // still incomplete but we have successfully written, no need to do anything else
                        // or we're complete but unable to write anything else
                        return true;
                    }
                }


                if (nextIndex) {
                    if (!tailer.nextIndex()) {
                        pauser.pause();
                        if (!tailer.nextIndex()) {
                            return false;
                        }
                    }
                } else {
                    if (!tailer.index(this.index)) {
                        return false;

                    } else {
                        this.nextIndex = true;
                    }
                }

                final MappingProvider mappingProvider = sessionDetailsProvider.get(MappingProvider.class);
                content = applyMapping(tailer, mappingProvider);
                int size = (int) content.remaining();

                if (out.remaining() >= ChronicleTcp.HEADER_SIZE) {
                    writeSizeAndIndex(out, (int) content.limit(), tailer.index());

                    // for large objects send one at a time.
                    if (size > out.remaining()) {
                        writePartial(out);
                    } else {
                        out.write(content);

                        long previousIndex = tailer.index();
                        long currentIndex;
                        for (int count = maxExcerptsPerMessage; (count > 0) && tailer.nextIndex(); ) {
                            currentIndex = tailer.index();
                            content = applyMapping(tailer, mappingProvider);

                            // if there is free space, copy another one.
                            if (hasRoomForExcerpt(out, content)) {
                                size = (int) content.limit();
                                previousIndex = currentIndex;
                                writeSizeAndIndex(out, size, currentIndex);
                                out.write(content);
                                count--;

                                tailer.finish();
                            } else {
                                tailer.finish();
                                // if there is no space, go back to the previous index
                                tailer.index(previousIndex);
                                break;
                            }
                        }

                        state = TcpHandlerState.EXCERPT_COMPLETE;

                    }

                    return true;
                } else {
                    if (tailer.index(tailer.index())) {
                        index = tailer.index();
                        nextIndex = false;
                    }
                    return false;
                }
            }

            @Override
            protected boolean onSubscribe(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
                if (in.remaining() >= 8) {
                    this.index = in.readLong();
                    if (this.index == ChronicleTcp.IDX_TO_START) {
                        this.nextIndex = true;
                        this.tailer = tailer.toStart();
                        this.index = -1;

                    } else if (this.index == ChronicleTcp.IDX_TO_END) {
                        this.nextIndex = false;
                        this.tailer = tailer.toEnd();
                        this.index = tailer.index();

                        if (this.index == -1) {
                            this.nextIndex = true;
                            this.tailer = tailer.toStart();
                            this.index = -1;
                        }
                    } else {
                        this.nextIndex = false;
                    }

                    writeSizeAndIndex(out, ChronicleTcp.SYNC_IDX_LEN, this.index);

                    subscriptionListener.subscribed();
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            protected boolean onSubmit(Bytes in, Bytes out, boolean ack) throws IOException {
                if (in.remaining() >= 8) {
                    long size = in.readLong();
                    if (in.remaining() >= size) {
                        long inLimit = in.limit();
                        in.limit(in.position() + size);

                        try {
                            appender.startExcerpt((int) size);
                            appender.write(in);
                            appender.finish();
                        } finally {
                            in.limit(inLimit);
                        }

                        pauser.unpause();

                        if (ack) {
                            writeSizeAndIndex(out, ChronicleTcp.ACK_LEN, appender.lastWrittenIndex());
                        }

                        return true;
                    } else {
                        in.position(in.position() - 8); // unread the size
                    }
                }
                return false;
            }

        }

        public static IndexedSourceTcpHandler indexed() {
            return new IndexedSourceTcpHandler();
        }

        public static class IndexedSourceTcpHandler extends SourceTcpHandler {

            @Override
            protected boolean write(Bytes out, SessionDetailsProvider sessionDetailsProvider) {
                if (state == TcpHandlerState.EXCERPT_INCOMPLETE) {
                    writePartial(out);
                    if (state == TcpHandlerState.EXCERPT_INCOMPLETE || out.remaining() <= ChronicleTcp.HEADER_SIZE) {
                        // still incomplete but we have successfully written, no need to do anything else
                        // or we're complete but unable to write anything else
                        return true;
                    }
                }

                if (!tailer.index(index)) {
                    if (tailer.wasPadding()) {
                        if (index >= 0) {
                            writeSizeAndIndex(out, ChronicleTcp.PADDED_LEN, tailer.index());
                        }

                        index++;
                    }

                    pauser.pause();

                    if (!tailer.index(index)) {
                        return false;
                    }
                }

                pauser.reset();

                final MappingProvider mappingProvider = sessionDetailsProvider.get(MappingProvider.class);
                content = applyMapping(tailer, mappingProvider);
                int size = (int) content.limit();

                if (out.remaining() >= ChronicleTcp.HEADER_SIZE) {
                    writeSizeAndIndex(out, size, tailer.index());

                    // for large objects send one at a time.
                    if (content.remaining() > out.remaining()) {
                        writePartial(out);
                    } else {
                        out.write(content);
                        for (int count = maxExcerptsPerMessage; (count > 0) && tailer.index(index + 1); ) {
                            if (!tailer.wasPadding()) {
                                content = applyMapping(tailer, mappingProvider);
                                // if there is free space, copy another one.
                                if (hasRoomForExcerpt(out, content)) {
                                    size = (int) content.limit();
                                    writeSizeAndIndex(out, size, tailer.index());
                                    out.write(content);

                                    index++;
                                    count--;

                                    tailer.finish();

                                } else {
                                    break;
                                }
                            } else {
                                if (hasRoomFor(out, ChronicleTcp.HEADER_SIZE)) {
                                    writeSizeAndIndex(out, ChronicleTcp.PADDED_LEN, index);
                                } else {
                                    break;
                                }

                                index++;
                            }
                        }
                    }

                    index++;
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            protected boolean onSubscribe(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
                this.index = in.readLong();
                if (this.index == ChronicleTcp.IDX_TO_START) {
                    this.index = -1;

                } else if (this.index == ChronicleTcp.IDX_TO_END) {
                    this.index = tailer.toEnd().index();
                }

                writeSizeAndIndex(out, ChronicleTcp.SYNC_IDX_LEN, this.index);

                subscriptionListener.subscribed();
                return true;
            }

            @Override
            protected boolean onSubmit(Bytes in, Bytes out, boolean ack) throws IOException {
                if (ack) {
                    writeSizeAndIndex(out, ChronicleTcp.NACK_LEN, ChronicleTcp.IDX_NOT_SUPPORTED);
                }

                return true;
            }
        }

        public interface SubscriptionListener {
            void subscribed();

            void unSubscribed();
        }

    }

    public enum TcpHandlerState {
        EXCERPT_COMPLETE,
        EXCERPT_INCOMPLETE
    }

}
