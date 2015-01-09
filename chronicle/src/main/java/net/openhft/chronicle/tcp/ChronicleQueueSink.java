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
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.tools.WrappedChronicle;
import net.openhft.chronicle.tools.WrappedExcerpt;
import net.openhft.chronicle.tools.WrappedExcerptAppender;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;

public class ChronicleQueueSink extends WrappedChronicle {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChronicleQueueSink.class);

    private final SinkTcp connection;
    private final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;
    private final boolean isLocal;
    private volatile boolean closed;
    private ExcerptCommon excerpt;

    public ChronicleQueueSink(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, final SinkTcp connection) {
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

        this.excerpt = isLocal
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
            ChronicleQueueSink.this.excerpt = null;
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
            super.wrappedAppender.addPaddedEntry();
        }

        @Override
        public void startExcerpt(long capacity, long index) {
            super.wrappedAppender.startExcerpt(capacity);
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
