/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.tcp;

import net.openhft.chronicle.*;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;

public class InMemoryChronicleSink extends ChronicleSink {
    private final ChronicleSinkConfig config;
    private final InetSocketAddress address;
    private final Logger logger;
    private final List<ExcerptCommon> excerpts;
    private final ChronicleType chronicleType;

    private volatile boolean closed;

    public InMemoryChronicleSink(final ChronicleType type, @NotNull String host, int port) {
        this(type, new InetSocketAddress(host, port), ChronicleSinkConfig.DEFAULT);
    }

    public InMemoryChronicleSink(final ChronicleType type, @NotNull String host, int port, @NotNull final ChronicleSinkConfig config) {
        this(type, new InetSocketAddress(host, port), config);
    }

    public InMemoryChronicleSink(final ChronicleType type, @NotNull final InetSocketAddress address) {
        this(type, address, ChronicleSinkConfig.DEFAULT);
    }

    public InMemoryChronicleSink(final ChronicleType type, @NotNull final InetSocketAddress address, @NotNull final ChronicleSinkConfig config) {
        this.config = config;
        this.address = address;
        this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + address.toString());
        this.excerpts = new LinkedList<ExcerptCommon>();
        this.closed = false;
        this.chronicleType = type;
    }


    @Override
    public String name() {
        return getClass().getName() + "@" + address.toString();
    }

    @Override
    public synchronized void  close() throws IOException {
        if(!closed) {
            closed = true;

            for (ExcerptCommon excerpt : excerpts) {
                excerpt.close();
            }

            excerpts.clear();
        }
    }

    @Override
    public void clear() {
        try {
            close();
        } catch (IOException e) {
            logger.warn("Error closing Sink", e);
        }
    }

    @Override
    public synchronized Excerpt createExcerpt() throws IOException {
        Excerpt excerpt = this.chronicleType == ChronicleType.INDEXED
            ? new InMemoryIndexedExcerpt()
            : new InMemoryVanillaExcerpt();

        excerpts.add(excerpt);

        return excerpt;
    }

    @Override
    public synchronized ExcerptTailer createTailer() throws IOException {
        ExcerptTailer excerpt = this.chronicleType == ChronicleType.INDEXED
            ? new InMemoryIndexedExcerptTailer()
            : new InMemoryVanillaExcerptTailer();

        excerpts.add(excerpt);

        return excerpt;
    }

    @Override
    public ExcerptAppender createAppender() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long lastWrittenIndex() {
        return 0;
    }

    @Override
    public long size() {
        return 0;
    }

    // *************************************************************************
    // Excerpt
    // *************************************************************************

    private abstract class AbstractInMemoryExcerpt extends NativeBytes implements ExcerptCommon {
        protected final Logger logger;
        protected long index;
        protected int lastSize;
        protected final ByteBuffer buffer;
        private SocketChannel channel;

        public AbstractInMemoryExcerpt() {
            super(NO_PAGE, NO_PAGE);

            this.buffer = TcpUtil.createBuffer(config.minBufferSize(), ByteOrder.nativeOrder());
            this.startAddr = ((DirectBuffer) this.buffer).address();
            this.capacityAddr = this.startAddr + config.minBufferSize();
            this.index = -1;
            this.lastSize = 0;
            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + address.toString());
            this.channel = null;
        }

        @Override
        public void finish() {
            if(lastSize > 0) {
                buffer.position(buffer.position() + lastSize);
            }

            super.finish();
        }

        @Override
        public void close() {
            if (channel != null) {
                try {
                    channel.close();
                    channel = null;
                } catch (IOException e) {
                    logger.warn("Error closing socket", e);
                }
            }
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
            return index();
        }

        @Override
        public Chronicle chronicle() {
            return InMemoryChronicleSink.this;
        }

        protected void openChannel() {
            while (!closed) {
                try {
                    buffer.clear();
                    buffer.limit(0);

                    channel = SocketChannel.open(address);
                    channel.socket().setReceiveBufferSize(config.minBufferSize());
                    logger.info("Connected to " + address);

                    return;
                } catch (IOException e) {
                    logger.info("Failed to connect to {}, retrying", address, e);
                }

                try {
                    Thread.sleep(config.reconnectDelay());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        public boolean isChannelOpen() {
            return !closed && channel!= null && channel.isOpen();
        }

        protected boolean writeToChannel(final ByteBuffer buffer) {
            try {
                TcpUtil.writeAllOrEOF(channel, buffer);
            } catch (IOException e) {
                return false;
            }

            return true;
        }

        protected boolean readFromChannel(int size) throws IOException {
            if(!closed) {
                if (buffer.remaining() < size) {
                    if (buffer.remaining() == 0) {
                        buffer.clear();
                    } else {
                        buffer.compact();
                    }

                    while (buffer.position() < size) {
                        if (channel.read(buffer) < 0) {
                            channel.close();
                            return false;
                        }
                    }

                    buffer.flip();
                }
            }

            return !closed;
        }
    }

    // *************************************************************************
    // INDEXED
    // *************************************************************************

    private class InMemoryIndexedExcerptTailer extends AbstractInMemoryExcerpt implements ExcerptTailer {
        @Override
        public ExcerptTailer toStart() {
            index(-1);
            return this;
        }

        @Override
        public ExcerptTailer toEnd() {
            index(-2);
            return this;
        }

        @Override
        public boolean index(long index) {
            this.index = index;
            this.lastSize = 0;

            try {
                if(!isChannelOpen()) {
                    openChannel();
                }

                if (writeToChannel(ByteBuffer.allocate(8).putLong(0, this.index))) {
                    while (readFromChannel(TcpUtil.HEADER_SIZE)) {
                        buffer.mark();
                        int excerptSize = buffer.getInt();
                        switch(excerptSize) {
                            case InProcessChronicleSource.PADDED_LEN:
                            case InProcessChronicleSource.IN_SYNC_LEN:
                                buffer.getLong();
                                return false;
                        }

                        long receivedIndex = buffer.getLong();
                        if(index == -2 || receivedIndex == index + 1) {
                            buffer.reset();
                            if(nextIndex()) {
                                finish();
                                lastSize = 0;
                                return true;
                            } else {
                                return false;
                            }
                        }

                        if(buffer.remaining() >= excerptSize) {
                            buffer.position(buffer.position() + excerptSize);
                        }
                    }
                }
            } catch (IOException e) {
                logger.warn("",e);
            }

            return false;
        }

        @Override
        public boolean nextIndex() {
            try {
                if(!isChannelOpen()) {
                    return index(this.index);
                }

                if(!readFromChannel(TcpUtil.HEADER_SIZE + 8)) {
                    return false;
                }

                int excerptSize = buffer.getInt();
                switch (excerptSize) {
                    case InProcessChronicleSource.IN_SYNC_LEN:
                    case InProcessChronicleSource.PADDED_LEN:
                        buffer.getLong();
                        return false;
                }

                if (excerptSize > 128 << 20 || excerptSize < 0) {
                    throw new StreamCorruptedException("Size was " + excerptSize);
                }

                index = buffer.getLong();
                if(buffer.remaining() < excerptSize) {
                    if(!readFromChannel(buffer.remaining() - excerptSize)) {
                        return false;
                    }
                }

                positionAddr = startAddr + buffer.position();
                limitAddr = startAddr + buffer.limit();
                lastSize = excerptSize;
            } catch (IOException e) {
                close();
                return false;
            }

            return true;
        }
    }

    private class InMemoryIndexedExcerpt extends InMemoryIndexedExcerptTailer implements Excerpt {
        @Override
        public long findMatch(@NotNull ExcerptComparator comparator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void findRange(@NotNull long[] startEnd, @NotNull ExcerptComparator comparator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Excerpt toStart() {
            super.toStart();
            return this;
        }

        @Override
        public Excerpt toEnd() {
            super.toEnd();
            return this;
        }
    }

    // *************************************************************************
    // VANILLA
    // *************************************************************************

    private class InMemoryVanillaExcerptTailer extends AbstractInMemoryExcerpt implements ExcerptTailer {
        @Override
        public ExcerptTailer toStart() {
            index(-1);
            return this;
        }

        @Override
        public ExcerptTailer toEnd() {
            index(-2);
            return this;
        }

        @Override
        public boolean index(long index) {
            this.index = index;
            this.lastSize = 0;

            try {
                if(!isChannelOpen()) {
                    openChannel();
                }

                if (writeToChannel(ByteBuffer.allocate(8).putLong(0, this.index))) {
                    while (readFromChannel(TcpUtil.HEADER_SIZE)) {
                        buffer.mark();
                        int excerptSize = buffer.getInt();
                        switch(excerptSize) {
                            case InProcessChronicleSource.IN_SYNC_LEN:
                            case InProcessChronicleSource.PADDED_LEN:
                                buffer.getLong();
                                return false;
                        }

                        long receivedIndex = buffer.getLong();
                        if(index == -2 || receivedIndex == index) {
                            buffer.reset();
                            if(nextIndex()) {
                                finish();
                                lastSize = 0;
                                return true;
                            } else {
                                return false;
                            }
                        } else if(index == -1) {
                            buffer.reset();
                            if(nextIndex()) {
                                finish();
                                lastSize = 0;
                                return true;
                            } else {
                                return false;
                            }
                        }

                        if(buffer.remaining() >= excerptSize) {
                            buffer.position(buffer.position() + excerptSize);
                        }
                    }
                }
            } catch (IOException e) {
                logger.warn("",e);
            }

            return false;
        }

        @Override
        public boolean nextIndex() {
            try {
                if(!isChannelOpen()) {
                    return index(this.index);
                }

                if(!readFromChannel(TcpUtil.HEADER_SIZE + 8)) {
                    return false;
                }

                int excerptSize = buffer.getInt();
                switch (excerptSize) {
                    case InProcessChronicleSource.IN_SYNC_LEN:
                    case InProcessChronicleSource.PADDED_LEN:
                        buffer.getLong();
                        return false;
                    default:
                        break;
                }

                if (excerptSize > 128 << 20 || excerptSize < 0) {
                    throw new StreamCorruptedException("Size was " + excerptSize);
                }

                index = buffer.getLong();
                if(buffer.remaining() < excerptSize) {
                    if(!readFromChannel(buffer.remaining() - excerptSize)) {
                        return false;
                    }
                }

                positionAddr = startAddr + buffer.position();
                limitAddr = startAddr + buffer.limit();
                lastSize = excerptSize;
            } catch (IOException e) {
                close();
                return false;
            }

            return true;
        }
    }

    private class InMemoryVanillaExcerpt extends InMemoryVanillaExcerptTailer implements Excerpt {
        @Override
        public long findMatch(@NotNull ExcerptComparator comparator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void findRange(@NotNull long[] startEnd, @NotNull ExcerptComparator comparator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Excerpt toStart() {
            super.toStart();
            return this;
        }

        @Override
        public Excerpt toEnd() {
            super.toEnd();
            return this;
        }
    }
}
