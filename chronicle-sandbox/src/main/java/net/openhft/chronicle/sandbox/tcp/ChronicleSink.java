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

package net.openhft.chronicle.sandbox.tcp;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptCommon;
import net.openhft.chronicle.ExcerptComparator;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.tools.WrappedExcerpt;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * This listens to a ChronicleSource and copies new entries. This Sink can be any
 * number of excerpt behind the source and can be restart many times without
 * losing data.
 *
 * <p>Can be used as a component with lower over head than ChronicleSink
 *
 * @author peter.lawrey
 */
public class ChronicleSink implements Chronicle {
    @NotNull
    private final Chronicle chronicle;

    @NotNull
    private final ChronicleSinkConfig config;

    @NotNull
    private final InetSocketAddress address;

    private final List<ExcerptCommon> excerpts;

    private final Logger logger;
    private volatile boolean closed = false;
    private final boolean isLocal;

    public ChronicleSink(String hostname, int port) throws IOException {
        this(null, ChronicleSinkConfig.DEFAULT, new InetSocketAddress(hostname, port));
    }

    public ChronicleSink(@NotNull Chronicle chronicle, String hostname, int port) throws IOException {
        this(chronicle, ChronicleSinkConfig.DEFAULT, new InetSocketAddress(hostname, port));
    }

    public ChronicleSink(@NotNull Chronicle chronicle, @NotNull final ChronicleSinkConfig config, String hostname, int port) throws IOException {
        this(chronicle, config, new InetSocketAddress(hostname, port));
    }

    public ChronicleSink(@NotNull final ChronicleSinkConfig config, String hostname, int port) throws IOException {
        this(null, config, new InetSocketAddress(hostname, port));
    }

    public ChronicleSink(@NotNull final InetSocketAddress address) throws IOException {
        this(null, ChronicleSinkConfig.DEFAULT, address);
    }

    public ChronicleSink(@NotNull final Chronicle chronicle, @NotNull final InetSocketAddress address) throws IOException {
        this(chronicle, ChronicleSinkConfig.DEFAULT, address);
    }

    public ChronicleSink(@NotNull final ChronicleSinkConfig config, @NotNull final InetSocketAddress address) throws IOException {
        this(null, config, address);
    }

    public ChronicleSink(@NotNull final Chronicle chronicle, @NotNull final ChronicleSinkConfig config, @NotNull final InetSocketAddress address) throws IOException {
        this.chronicle = chronicle;
        this.config = config;
        this.address = address;
        this.logger = LoggerFactory.getLogger(getClass().getName() + '.' + address.getHostName() + '@' + address.getPort());
        this.excerpts = Collections.synchronizedList(new LinkedList<ExcerptCommon>());
        this.isLocal = config.sharedChronicle() && ChronicleTcp.isLocalhost(this.address.getAddress());
    }

    @Override
    public String name() {
        return chronicle.name();
    }

    @NotNull
    @Override
    public synchronized Excerpt createExcerpt() throws IOException {
        Excerpt excerpt = null;

        if(chronicle == null) {
            excerpt = new VolatileExcerpt();
        } else {
            excerpt = (Excerpt)createPersistedExcerpt();
        }

        if(excerpt != null) {
            excerpts.add(excerpt);
        }

        return excerpt;
    }

    @NotNull
    @Override
    public synchronized ExcerptTailer createTailer() throws IOException {
        ExcerptTailer excerpt = null;

        if(chronicle == null) {
            excerpt = new VolatileExcerptTailer();
        } else {
            excerpt = createPersistedExcerpt();
        }

        if(excerpt != null) {
            excerpts.add(excerpt);
        }

        return excerpt;
    }

    @NotNull
    @Override
    public ExcerptAppender createAppender() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long lastIndex() {
        return (chronicle != null) ? chronicle.lastIndex() : -1;
    }

    @Override
    public long lastWrittenIndex() {
        return (chronicle != null) ? chronicle.lastWrittenIndex() : -1;
    }

    @Override
    public long size() {
        return (chronicle != null) ? chronicle.size() : 0;
    }

    @Override
    public void clear() {
        if(chronicle != null) {
            chronicle.clear();
        }
    }

    @Override
    public synchronized void close() {
        if(!closed) {
            closed = true;

            synchronized (excerpts) {
                for (final ExcerptCommon excerpt : excerpts) {
                    excerpt.close();
                }
            }

            try {
                if(chronicle != null) {
                    chronicle.close();
                }
            } catch (IOException e) {
                logger.warn("Error closing Sink", e);
            }
        }
    }

    public void checkCounts(int min, int max) {
        if(chronicle instanceof VanillaChronicle) {
            ((VanillaChronicle)chronicle).checkCounts(min, max);
        }
    }

    private ExcerptTailer createPersistedExcerpt() throws IOException {
        if (!excerpts.isEmpty()) {
            throw new UnsupportedOperationException("An Excerpt has already been created");
        }

        if(isLocal) {
            return (chronicle instanceof IndexedChronicle)
                ? new PersistentIndexedLocalSinkExcerpt(chronicle.createTailer())
                : new PersistentVanillaLocalSinkExcerpt(chronicle.createTailer());
        } else {
            return (chronicle instanceof IndexedChronicle)
                ? new PersistentIndexedSinkExcerpt(chronicle.createTailer())
                : new PersistentVanillaSinkExcerpt(chronicle.createTailer());
        }
    }

    protected Chronicle chronicle() {
        return this.chronicle;
    }

    protected boolean closed() {
        return this.closed;
    }

    protected ChronicleSinkConfig config() {
        return this.config;
    }

    // *************************************************************************
    //
    // *************************************************************************

    private final class SinkConnector {
        private final ByteBuffer buffer;
        private SocketChannel channel;

        public SinkConnector() {
            this.channel = null;
            this.buffer = ChronicleTcp.createBuffer(config.minBufferSize(), ByteOrder.nativeOrder());
        }

        public ByteBuffer buffer() {
            return buffer;
        }

        public void close() throws IOException {
            if(channel != null) {
                channel.close();
                channel = null;
            }
        }

        public boolean open() {
            while (!closed) {
                try {
                    buffer.clear();
                    buffer.limit(0);

                    this.channel = SocketChannel.open(address);
                    this.channel.socket().setTcpNoDelay(true);
                    this.channel.socket().setReceiveBufferSize(config.minBufferSize());
                    logger.info("Connected to " + address);

                    return true;
                } catch (IOException e) {
                    logger.info("Failed to connect to {}, retrying", address, e);
                }

                try {
                    Thread.sleep(config.reconnectDelay());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            return false;
        }

        public boolean isOpen() {
            return !closed && channel!= null && channel.isOpen();
        }

        public boolean isClosed() {
            return !isOpen();
        }

        public boolean write(final ByteBuffer buffer) {
            try {
                ChronicleTcp.writeAllOrEOF(channel, buffer);
            } catch (IOException e) {
                return false;
            }

            return true;
        }

        public boolean read() throws IOException {
            if (channel.read(buffer) < 0) {
                throw new EOFException();
            }

            return true;
        }

        public boolean read(int size) throws IOException {
            return read(size, size);
        }

        public boolean read(int threshod, int size) throws IOException {
            if(!closed) {
                int rem = buffer.remaining();
                if (rem < threshod) {
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
    // TCP/DISK based replication
    // *************************************************************************

    private abstract class AbstractPersistentSinkExcerpt<T extends Chronicle>  extends WrappedExcerpt {
        protected final T chronicleImpl;
        protected final ByteBuffer buffer;
        protected final SinkConnector connector;
        protected long lastLocalIndex;

        public AbstractPersistentSinkExcerpt(ExcerptCommon excerptCommon) {
            super(excerptCommon);
            this.chronicleImpl = (T)chronicle;
            this.connector = new SinkConnector();
            this.buffer = this.connector.buffer();
            this.lastLocalIndex = -1;
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
                connector.close();
            } catch (IOException e) {
                logger.warn("Error closing socketChannel", e);
            }

            if(!closed) {
                synchronized (excerpts) {
                    excerpts.remove(this);
                }
            }

            super.close();
        }

        protected boolean readNext() {
            if (!connector.isOpen()) {
                if(connector.open()) {
                    if(!isLocal) {
                        final ChronicleTcp.Command command =
                            ChronicleTcp.Command.make(
                                ChronicleTcp.Command.ACTION_SUBSCRIBE,
                                lastLocalIndex());
                        try {
                            command.write(connector.channel);
                            lastLocalIndex = command.data();
                        } catch (IOException e) {
                            return false;
                        }
                    }
                } else {
                    return false;
                }
            }

            return connector.isOpen() && readNextExcerpt();
        }

        protected abstract long lastLocalIndex();
        protected abstract boolean readNextExcerpt();
    }

    /**
     * Local IndexedChronicle synk
     */
    private class PersistentIndexedLocalSinkExcerpt extends AbstractPersistentSinkExcerpt<IndexedChronicle> {
        @SuppressWarnings("unchecked")
        public PersistentIndexedLocalSinkExcerpt(@NotNull final ExcerptCommon excerpt) throws IOException {
            super(excerpt);
        }

        @Override
        protected long lastLocalIndex()  {
            return chronicleImpl.lastWrittenIndex();
        }

        @Override
        protected boolean readNextExcerpt() {
            try {
                if (!closed) {
                    ChronicleTcp.Command.makeAndSend(
                        ChronicleTcp.Command.ACTION_QUERY,
                        lastLocalIndex(),
                        connector.channel
                    );

                    if (connector.read(ChronicleTcp.HEADER_SIZE)) {
                        final int size = buffer.getInt();
                        final long scIndex = buffer.getLong();

                        switch (size) {
                            case ChronicleTcp.IN_SYNC_LEN:
                                return false;
                            case ChronicleTcp.PADDED_LEN:
                                return false;
                            case ChronicleTcp.SYNC_IDX_LEN:
                                return true;
                        }
                    }
                }
            } catch (IOException e) {
                logger.info("Lost connection to {} retrying", address, e);
                try {
                    connector.close();
                } catch (IOException ignored) {
                }
            }

            return false;
        }
    }

    /**
     * IndexedChronicle sink
     */
    private final class PersistentIndexedSinkExcerpt extends AbstractPersistentSinkExcerpt<IndexedChronicle> {
        @NotNull
        private final ExcerptAppender appender;

        @SuppressWarnings("unchecked")
        public PersistentIndexedSinkExcerpt(@NotNull final ExcerptCommon excerpt) throws IOException {
            super(excerpt);
            this.appender = chronicle.createAppender();
        }

        @Override
        protected long lastLocalIndex()  {
            return chronicleImpl.lastWrittenIndex();
        }

        @Override
        protected boolean readNextExcerpt() {
            try {
                if(!closed && !connector.read(ChronicleTcp.HEADER_SIZE, ChronicleTcp.HEADER_SIZE + 8)) {
                    return false;
                }

                final int size = buffer.getInt();
                final long scIndex = buffer.getLong();

                switch (size) {
                    case ChronicleTcp.IN_SYNC_LEN:
                        return false;
                    case ChronicleTcp.PADDED_LEN:
                        appender.startExcerpt(((IndexedChronicle) chronicle).builder().dataBlockSize() - 1);
                        return true;
                    case ChronicleTcp.SYNC_IDX_LEN:
                        //Sync IDX message, re-try
                        return readNextExcerpt();
                }

                if (size > 128 << 20 || size < 0) {
                    throw new StreamCorruptedException("size was " + size);
                }
                if (scIndex != chronicle.size()) {
                    throw new StreamCorruptedException("Expected index " + chronicle.size() + " but got " + scIndex);
                }

                appender.startExcerpt(size);
                // perform a progressive copy of data.
                long remaining = size;
                int limit = buffer.limit();

                int size2 = (int) Math.min(buffer.remaining(), remaining);
                remaining -= size2;
                buffer.limit(buffer.position() + size2);
                appender.write(buffer);
                // reset the limit;
                buffer.limit(limit);

                // needs more than one read.
                while (remaining > 0) {
                    buffer.clear();
                    int size3 = (int) Math.min(buffer.capacity(), remaining);
                    buffer.limit(size3);
                    connector.read();
                    buffer.flip();
                    remaining -= buffer.remaining();
                    appender.write(buffer);
                }

                appender.finish();
            } catch (IOException e) {
                logger.info("Lost connection to {} retrying", address, e);
                try {
                    connector.close();
                } catch (IOException ignored) {
                }
            }

            return true;
        }

        @Override
        public void close() {
            if(this.appender != null) {
                this.appender.close();
            }

            super.close();
        }
    }

    /**
     * Local VanillaChronicle synk
     */
    private class PersistentVanillaLocalSinkExcerpt extends AbstractPersistentSinkExcerpt<VanillaChronicle> {
        @SuppressWarnings("unchecked")
        public PersistentVanillaLocalSinkExcerpt(@NotNull final ExcerptCommon excerpt) throws IOException {
            super(excerpt);
        }

        @Override
        protected long lastLocalIndex()  {
            return chronicleImpl.lastIndex();
        }

        @Override
        protected boolean readNextExcerpt() {
            try {
                if (!closed) {
                    ChronicleTcp.Command.makeAndSend(
                        ChronicleTcp.Command.ACTION_QUERY,
                        lastLocalIndex(),
                        connector.channel
                    );

                    if (connector.read(ChronicleTcp.HEADER_SIZE)) {
                        final int size = buffer.getInt();
                        final long scIndex = buffer.getLong();

                        switch (size) {
                            case ChronicleTcp.IN_SYNC_LEN:
                                return false;
                            case ChronicleTcp.PADDED_LEN:
                                return false;
                            case ChronicleTcp.SYNC_IDX_LEN:
                                return true;
                        }
                    }
                }
            } catch (IOException e) {
                logger.info("Lost connection to {} retrying", address, e);
                try {
                    connector.close();
                } catch (IOException ignored) {
                }
            }

            return false;
        }
    }

    /**
     * VanillaChronicle sink
     */
    private final class PersistentVanillaSinkExcerpt extends AbstractPersistentSinkExcerpt<VanillaChronicle> {
        @NotNull
        private final VanillaChronicle.VanillaAppender appender;

        @SuppressWarnings("unchecked")
        public PersistentVanillaSinkExcerpt(@NotNull final ExcerptCommon excerpt) throws IOException {
            super(excerpt);
            this.appender = chronicleImpl.createAppender();
        }

        @Override
        protected long lastLocalIndex()  {
            return chronicleImpl.lastIndex();
        }

        @Override
        protected boolean readNextExcerpt() {
            try {
                if(!closed && !connector.read(ChronicleTcp.HEADER_SIZE, ChronicleTcp.HEADER_SIZE + 8)) {
                    return false;
                }

                final int size = buffer.getInt();
                final long scIndex = buffer.getLong();

                switch (size) {
                    case ChronicleTcp.IN_SYNC_LEN:
                        //Heartbeat message ignore and return false
                        return false;
                    case ChronicleTcp.PADDED_LEN:
                        //Padded message, should not happen
                        return false;
                    case ChronicleTcp.SYNC_IDX_LEN:
                        //Sync IDX message, re-try
                        return readNextExcerpt();
                }

                if (size > 128 << 20 || size < 0) {
                    throw new StreamCorruptedException("size was " + size);
                }

                if(lastLocalIndex != scIndex) {
                    int cycle = (int) (scIndex >>> chronicleImpl.getEntriesForCycleBits());

                    appender.startExcerpt(size, cycle);
                    // perform a progressive copy of data.
                    long remaining = size;
                    int limit = buffer.limit();
                    int size2 = (int) Math.min(buffer.remaining(), remaining);
                    remaining -= size2;
                    buffer.limit(buffer.position() + size2);
                    appender.write(buffer);
                    // reset the limit;
                    buffer.limit(limit);

                    // needs more than one read.
                    while (remaining > 0) {
                        buffer.clear();
                        int size3 = (int) Math.min(buffer.capacity(), remaining);
                        buffer.limit(size3);
                        connector.read();
                        buffer.flip();
                        remaining -= buffer.remaining();
                        appender.write(buffer);
                    }

                    appender.finish();
                } else {
                    buffer.position(buffer.position() + size);
                    return readNextExcerpt();
                }
            } catch (IOException e) {
                logger.info("Lost connection to {} retrying ",address, e);
                try {
                    connector.close();
                } catch (IOException ignored) {
                }
            }

            return true;
        }

        @Override
        public void close() {
            if(this.appender != null) {
                this.appender.close();
            }

            super.close();
        }
    }

    // *************************************************************************
    // TCP/VOLATILE based replication
    // *************************************************************************

    private class VolatileExcerptTailer extends NativeBytes implements ExcerptTailer {
        private final Logger logger;
        private long index;
        private int lastSize;
        private final ByteBuffer buffer;
        private final SinkConnector connector;

        public VolatileExcerptTailer() {
            super(NO_PAGE, NO_PAGE);

            this.index = -1;
            this.lastSize = 0;
            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + address.toString());
            this.connector = new SinkConnector();
            this.buffer = this.connector.buffer();
            this.startAddr = ((DirectBuffer) this.buffer).address();
            this.capacityAddr = this.startAddr + config.minBufferSize();
        }

        @Override
        public void finish() {
            if(!isFinished()) {
                if (lastSize > 0) {
                    buffer.position(buffer.position() + lastSize);
                }

                super.finish();
            }
        }

        @Override
        public synchronized void close() {
            try {
                connector.close();
            } catch (IOException e) {
                logger.warn("Error closing socketChannel", e);
            }

            synchronized (excerpts) {
                excerpts.remove(this);
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
            return ChronicleSink.this;
        }

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
                if(!connector.isOpen()) {
                    connector.open();
                }

                final ChronicleTcp.Command command = ChronicleTcp.Command.make(
                    ChronicleTcp.Command.ACTION_SUBSCRIBE,
                    this.index);

                if(command.write(connector.channel)) {
                    while (connector.read(ChronicleTcp.HEADER_SIZE)) {
                        int receivedSize = buffer.getInt();
                        long receivedIndex = buffer.getLong();

                        switch(receivedSize) {
                            case ChronicleTcp.SYNC_IDX_LEN:
                                if(index == -1) {
                                    return receivedIndex == -1;
                                } else if(index == -2) {
                                    return advanceIndex();
                                } else {
                                    return (index == receivedIndex) ? advanceIndex() : false;
                                }
                            case ChronicleTcp.PADDED_LEN:
                            case ChronicleTcp.IN_SYNC_LEN:
                                return false;
                        }

                        if (buffer.remaining() >= receivedSize) {
                            buffer.position(buffer.position() + receivedSize);
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
                if(!connector.isOpen()) {
                    if(index(this.index)) {
                        return nextIndex();
                    } else {
                        return false;
                    }
                }

                if(!connector.read(ChronicleTcp.HEADER_SIZE + 8)) {
                    return false;
                }

                int excerptSize = buffer.getInt();
                long receivedIndex = buffer.getLong();

                switch (excerptSize) {
                    case ChronicleTcp.IN_SYNC_LEN:
                    case ChronicleTcp.PADDED_LEN:
                    case ChronicleTcp.SYNC_IDX_LEN:
                        return false;
                }

                if (excerptSize > 128 << 20 || excerptSize < 0) {
                    throw new StreamCorruptedException("Size was " + excerptSize);
                }

                if(buffer.remaining() < excerptSize) {
                    if(!connector.read(excerptSize)) {
                        return false;
                    }
                }

                index = receivedIndex;
                positionAddr = startAddr + buffer.position();
                limitAddr = positionAddr + excerptSize;
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
    }

    private class VolatileExcerpt extends VolatileExcerptTailer implements Excerpt {
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
