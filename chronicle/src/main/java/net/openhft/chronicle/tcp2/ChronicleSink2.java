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
package net.openhft.chronicle.tcp2;

import net.openhft.chronicle.*;
import net.openhft.chronicle.tcp.ChronicleSinkConfig;
import net.openhft.chronicle.tcp.ChronicleTcp;
import net.openhft.chronicle.tools.WrappedChronicle;
import net.openhft.chronicle.tools.WrappedExcerpt;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;

import net.openhft.chronicle.tcp2.AppenderAdapters.IndexedAppenderAdaper;
import net.openhft.chronicle.tcp2.AppenderAdapters.VanillaAppenderAdaper;

public class ChronicleSink2 extends WrappedChronicle {
    private final TcpConnection connection;
    private final ChronicleSinkConfig config;
    private final boolean isLocal;
    private volatile boolean closed;
    private ExcerptCommon excerpt;

    public ChronicleSink2(final Chronicle chronicle, final ChronicleSinkConfig config, final TcpConnection connection) {
        super(chronicle);
        this.connection = connection;
        this.config = config;
        this.closed = false;
        this.isLocal = config.sharedChronicle() && connection.isLocalhost();
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
        if( this.excerpt != null) {
            throw new IllegalStateException("An excerpt has already been created");
        }

        this.excerpt = delegatedChronicle == null
            ? new VolatileExcerpt()
            : isLocal
            ? new PersistentLocalSinkExcerpt(delegatedChronicle.createTailer())
            : new PersistentSinkExcerpt(delegatedChronicle.createTailer());

        return (Excerpt)this.excerpt;
    }

    @Override
    public synchronized ExcerptTailer createTailer() throws IOException {
        if( this.excerpt != null) {
            throw new IllegalStateException("A tailer has already been created");
        }

        this.excerpt = delegatedChronicle == null
            ? new VolatileExcerptTailer()
            : isLocal
                ? new PersistentLocalSinkExcerpt(delegatedChronicle.createTailer())
                : new PersistentSinkExcerpt(delegatedChronicle.createTailer());

        return (ExcerptTailer)this.excerpt;
    }

    @Override
    public ExcerptAppender createAppender() throws IOException {
        throw new UnsupportedOperationException();
    }

    // *************************************************************************
    // PERSISTED
    // *************************************************************************

    private abstract class AbstractPersistentSinkExcerpt extends WrappedExcerpt {
        protected final Logger logger;
        protected final ByteBuffer writeBuffer;
        protected final ByteBuffer readBuffer;

        protected AbstractPersistentSinkExcerpt(final ExcerptCommon excerptCommon) {
            super(excerptCommon);

            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + connection.toString());
            this.writeBuffer = ChronicleTcp2.createBuffer(16);
            this.readBuffer = ChronicleTcp2.createBuffer(config.minBufferSize());
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
            ChronicleSink2.this.excerpt = null;
        }

        protected void subscribe(long index) throws IOException {
            writeBuffer.clear();
            writeBuffer.putLong(ChronicleTcp2.ACTION_SUBSCRIBE);
            writeBuffer.putLong(index);
            writeBuffer.flip();

            connection.writeAllOrEOF(writeBuffer);
        }

        protected void query(long index) throws IOException {
            writeBuffer.clear();
            writeBuffer.putLong(ChronicleTcp2.ACTION_QUERY);
            writeBuffer.putLong(index);
            writeBuffer.flip();

            connection.writeAllOrEOF(writeBuffer);
        }

        protected abstract boolean readNext();
    }

    private class PersistentLocalSinkExcerpt extends AbstractPersistentSinkExcerpt {
        public PersistentLocalSinkExcerpt(final ExcerptCommon excerptCommon) {
            super(excerptCommon);
        }

        @Override
        protected boolean readNext() {
            if (!closed && !connection.isOpen()) {
                try {
                    connection.open();
                    readBuffer.clear();
                    readBuffer.limit(0);
                } catch (IOException e) {
                    logger.warn("Error closing socketChannel", e);
                    return false;
                }
            }

            return connection.isOpen() && readNextExcerpt();
        }

        private boolean readNextExcerpt() {
            try {
                if (!closed) {
                    query(delegatedChronicle.lastIndex());

                    if (connection.read(readBuffer, ChronicleTcp.HEADER_SIZE)) {
                        final int size = readBuffer.getInt();
                        final long scIndex = readBuffer.getLong();

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
            } catch (IOException e) {
                logger.warn("", e);
                close();
            }

            return false;
        }
    }

    private final class PersistentSinkExcerpt extends AbstractPersistentSinkExcerpt {

        private ExcerptAppender appender;
        private AppenderAdapter adapter;
        private long lastLocalIndex;

        public PersistentSinkExcerpt(final ExcerptCommon excerptCommon) {
            super(excerptCommon);

            this.appender = null;
            this.adapter = null;
            this.lastLocalIndex = -1;
        }

        @Override
        protected boolean readNext() {
            if (!closed && !connection.isOpen()) {
                try {
                    connection.open();
                    readBuffer.clear();
                    readBuffer.limit(0);

                    if(this.appender == null) {
                        this.appender = delegatedChronicle.createAppender();
                        this.adapter =
                            delegatedChronicle instanceof IndexedChronicle
                                ? new IndexedAppenderAdaper(delegatedChronicle, this.appender)
                                : new VanillaAppenderAdaper(delegatedChronicle, this.appender);
                    }

                    subscribe(lastLocalIndex = delegatedChronicle.lastIndex());
                } catch (IOException e) {
                    logger.warn("Error closing socketChannel", e);
                    return false;
                }
            }

            return connection.isOpen() && readNextExcerpt();
        }

        private boolean readNextExcerpt() {
            try {
                if(!closed && !connection.read(readBuffer, ChronicleTcp.HEADER_SIZE, ChronicleTcp.HEADER_SIZE + 8)) {
                    return false;
                }

                final int size = readBuffer.getInt();
                final long scIndex = readBuffer.getLong();

                switch (size) {
                    case ChronicleTcp.IN_SYNC_LEN:
                        //Heartbeat message ignore and return false
                        return false;
                    case ChronicleTcp.PADDED_LEN:
                        return this.adapter.handlePadding();
                    case ChronicleTcp.SYNC_IDX_LEN:
                        //Sync IDX message, re-try
                        return readNextExcerpt();
                }

                if (size > 128 << 20 || size < 0) {
                    throw new StreamCorruptedException("size was " + size);
                }

                if(lastLocalIndex != scIndex) {
                    this.adapter.startExcerpt(size, scIndex);

                    long remaining = size;
                    int limit = readBuffer.limit();
                    int size2 = (int) Math.min(readBuffer.remaining(), remaining);

                    remaining -= size2;
                    readBuffer.limit(readBuffer.position() + size2);
                    appender.write(readBuffer);
                    // reset the limit;
                    readBuffer.limit(limit);

                    // needs more than one read.
                    while (remaining > 0) {
                        readBuffer.clear();
                        int size3 = (int) Math.min(readBuffer.capacity(), remaining);
                        readBuffer.limit(size3);

                        connection.read(readBuffer);

                        readBuffer.flip();
                        remaining -= readBuffer.remaining();
                        appender.write(readBuffer);
                    }

                    appender.finish();
                } else {
                    readBuffer.position(readBuffer.position() + size);
                    return readNextExcerpt();
                }
            } catch (IOException e) {
                logger.warn("", e);
                close();
            }

            return true;
        }

        @Override
        public void close() {
            if(this.appender != null) {
                this.appender.close();

                this.appender = null;
                this.adapter = null;
            }

            super.close();
        }
    }

    // *************************************************************************
    // VOLATILE
    // *************************************************************************

    private class VolatileExcerptTailer extends NativeBytes implements ExcerptTailer {

        private final Logger logger;
        private final ByteBuffer writeBuffer;
        private final ByteBuffer readBuffer;

        private long index;
        private int lastSize;

        public VolatileExcerptTailer() {
            super(NO_PAGE, NO_PAGE);

            this.index = -1;
            this.lastSize = 0;
            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + connection.toString());
            this.writeBuffer = ChronicleTcp2.createBuffer(16);
            this.readBuffer = ChronicleTcp2.createBuffer(config.minBufferSize());
            this.startAddr = ((DirectBuffer) this.readBuffer).address();
            this.capacityAddr = this.startAddr + config.minBufferSize();
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
        public Chronicle chronicle() {
            return ChronicleSink2.this;
        }

        @Override
        public synchronized void close() {
            try {
                connection.close();
            } catch (IOException e) {
                logger.warn("Error closing socketChannel", e);
            }

            super.close();
            ChronicleSink2.this.excerpt = null;
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
                writeBuffer.putLong(ChronicleTcp2.ACTION_SUBSCRIBE);
                writeBuffer.putLong(this.index);
                writeBuffer.flip();

                connection.writeAllOrEOF(writeBuffer);

                while (connection.read(readBuffer, ChronicleTcp2.HEADER_SIZE)) {
                    int receivedSize = readBuffer.getInt();
                    long receivedIndex = readBuffer.getLong();

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
            try {
                if(!connection.isOpen()) {
                    if(index(this.index)) {
                        return nextIndex();
                    } else {
                        return false;
                    }
                }

                if(!connection.read(this.readBuffer, ChronicleTcp.HEADER_SIZE + 8)) {
                    return false;
                }

                int excerptSize = this.readBuffer.getInt();
                long receivedIndex = this.readBuffer.getLong();

                switch (excerptSize) {
                    case ChronicleTcp.IN_SYNC_LEN:
                    case ChronicleTcp.PADDED_LEN:
                    case ChronicleTcp.SYNC_IDX_LEN:
                        return false;
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
        public VolatileExcerpt() {
            super();
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
