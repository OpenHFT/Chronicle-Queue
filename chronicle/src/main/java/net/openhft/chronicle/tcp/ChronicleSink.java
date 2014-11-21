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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptCommon;
import net.openhft.chronicle.ExcerptComparator;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.tools.WrappedChronicle;
import net.openhft.chronicle.tools.WrappedExcerpt;
import net.openhft.chronicle.tools.WrappedExcerptAppender;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.io.serialization.impl.VanillaBytesMarshallerFactory;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;

public class ChronicleSink extends WrappedChronicle {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChronicleSink.class);

    private final SinkTcp connection;
    private final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;
    private final boolean isLocal;
    private volatile boolean closed;
    private ExcerptCommon excerpt;

    public ChronicleSink(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, final SinkTcp connection) {
        super(builder.chronicle());
        this.connection = connection;
        this.builder = builder;
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

        this.excerpt = wrappedChronicle == null
            ? new StatelessExcerpt()
            : isLocal
                ? new StatefulLocalExcerpt(wrappedChronicle.createTailer())
                : new StatefulExcerpt(wrappedChronicle.createTailer());

        return this.excerpt;
    }

    // *************************************************************************
    // STATEFULL
    // *************************************************************************

    private abstract class AbstractStatefulExcerpt extends WrappedExcerpt {
        protected final Logger logger;
        protected final ByteBuffer writeBuffer;
        protected final ByteBuffer readBuffer;

        protected AbstractStatefulExcerpt(final ExcerptCommon excerpt) {
            super(excerpt);

            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + connection.toString());
            this.writeBuffer = ChronicleTcp.createBuffer(16);
            this.readBuffer = ChronicleTcp.createBuffer(builder.minBufferSize());
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
            ChronicleSink.this.excerpt = null;
        }

        protected void subscribe(long index) throws IOException {
            writeBuffer.clear();
            writeBuffer.putLong(ChronicleTcp.ACTION_SUBSCRIBE);
            writeBuffer.putLong(index);
            writeBuffer.flip();

            connection.writeAllOrEOF(writeBuffer);
        }

        protected void query(long index) throws IOException {
            writeBuffer.clear();
            writeBuffer.putLong(ChronicleTcp.ACTION_QUERY);
            writeBuffer.putLong(index);
            writeBuffer.flip();

            connection.writeAllOrEOF(writeBuffer);
        }

        protected abstract boolean readNext();
    }

    private class StatefulLocalExcerpt extends AbstractStatefulExcerpt {

        public StatefulLocalExcerpt(final ExcerptCommon common) {
            super(common);
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
                    query(wrappedChronicle.lastIndex());

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

    private final class StatefulExcerpt extends AbstractStatefulExcerpt {

        //private ExcerptAppender appender;
        private AppenderAdapter adapter;
        private long lastLocalIndex;

        public StatefulExcerpt(final ExcerptCommon common) {
            super(common);

            //this.appender = null;
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

                    if(this.adapter == null) {
                        this.adapter = createAppenderAdapter();
                    }

                    subscribe(lastLocalIndex = wrappedChronicle.lastIndex());
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
                        this.adapter.writePaddedEntry();
                        return readNextExcerpt();
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
                    adapter.write(readBuffer);
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
                        adapter.write(readBuffer);
                    }

                    adapter.finish();
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
            if(this.adapter != null) {
                this.adapter.close();
                this.adapter = null;
            }

            super.close();
        }
    }

    // *************************************************************************
    // STATELESS
    // *************************************************************************

    private class StatelessExcerpt extends NativeBytes implements Excerpt {

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
            return ChronicleSink.this;
        }

        @Override
        public synchronized void close() {
            try {
                connection.close();
            } catch (IOException e) {
                logger.warn("Error closing socketChannel", e);
            }

            super.close();
            ChronicleSink.this.excerpt = null;
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

    // *************************************************************************
    // Appender adapters
    // *************************************************************************

    /**
     * Creates a SinkAppenderAdapter.
     *
     * @return  the SinkAppenderAdapter
     * @throws  java.io.IOException
     */
    private AppenderAdapter createAppenderAdapter() throws IOException {
        if(wrappedChronicle instanceof IndexedChronicle) {
            return new IndexedAppenderAdapter(wrappedChronicle, wrappedChronicle.createAppender());
        }

        if(wrappedChronicle instanceof VanillaChronicle) {
            return new VanillaAppenderAdapter(wrappedChronicle, wrappedChronicle.createAppender());
        }

        throw new IllegalArgumentException("Can only adapt Indexed or Vanilla chronicles");
    }


    private abstract class AppenderAdapter extends WrappedExcerptAppender {

        public AppenderAdapter(@NotNull ExcerptAppender appender) {
            super(appender);
        }

        public abstract void writePaddedEntry();
        public abstract void startExcerpt(long capacity, long index);
    }

    /**
     * IndexedChronicle AppenderAdapter
     */
    private final class IndexedAppenderAdapter extends AppenderAdapter {
        private final IndexedChronicle chronicle;

        public IndexedAppenderAdapter(@NotNull final Chronicle chronicle, @NotNull final ExcerptAppender appender) {
            super(appender);

            this.chronicle = (IndexedChronicle)chronicle;
        }

        @Override
        public void writePaddedEntry() {
            super.warappedAppender.addPaddedEntry();
        }

        @Override
        public void startExcerpt(long capacity, long index) {
            super.warappedAppender.startExcerpt(capacity);
        }
    }

    /**
     * VanillaChronicle AppenderAdapter
     */
    private final class VanillaAppenderAdapter extends AppenderAdapter {
        private final VanillaChronicle chronicle;
        private final VanillaChronicle.VanillaAppender appender;

        public VanillaAppenderAdapter(@NotNull final Chronicle chronicle, @NotNull final ExcerptAppender appender) {
            super(appender);

            this.chronicle = (VanillaChronicle)chronicle;
            this.appender = (VanillaChronicle.VanillaAppender)appender;
        }

        @Override
        public void writePaddedEntry() {
            LOGGER.warn("VanillaChronicle should not receive padded entries");
        }

        @Override
        public void startExcerpt(long capacity, long index) {
            int cycle = (int) (index >>> chronicle.getEntriesForCycleBits());
            this.appender.startExcerpt(capacity, cycle);
        }
    }
}
