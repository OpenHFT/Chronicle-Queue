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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.tcp.ChronicleSinkConfig;
import net.openhft.chronicle.tcp.ChronicleTcp;
import net.openhft.chronicle.tools.WrappedChronicle;
import net.openhft.lang.io.NativeBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ChronicleSink2 extends WrappedChronicle {
    private final TcpConnection cnx;

    public ChronicleSink2(final Chronicle chronicle, final TcpConnection cnx) {
        super(chronicle);
        this.cnx = cnx;
    }

    @Override
    public void close() throws IOException {
        if(this.cnx != null) {
            this.cnx.close();
        }

        super.close();
    }

    @Override
    public Excerpt createExcerpt() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new VolatileExcerptTailer(this.cnx);
    }

    @Override
    public ExcerptAppender createAppender() throws IOException {
        throw new UnsupportedOperationException();
    }

    private class VolatileExcerptTailer extends NativeBytes implements ExcerptTailer {

        private final Logger logger;
        private final ByteBuffer writeBuffer;
        private final ByteBuffer readBuffer;
        private final ChronicleSinkConfig config;
        private final TcpConnection connection;

        private long index;
        private int lastSize;

        public VolatileExcerptTailer(final TcpConnection connection) {
            this(ChronicleSinkConfig.DEFAULT, connection);
        }

        public VolatileExcerptTailer(final ChronicleSinkConfig config, final TcpConnection connection) {
            super(NO_PAGE, NO_PAGE);

            this.index = -1;
            this.lastSize = 0;
            this.config = config;
            this.connection = connection;
            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + connection.toString());
            this.writeBuffer = ChronicleTcp2.createBuffer(16, ByteOrder.nativeOrder());
            this.readBuffer = ChronicleTcp2.createBuffer(config.minBufferSize(), ByteOrder.nativeOrder());
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
                writeBuffer.putLong(ChronicleTcp.Command.ACTION_SUBSCRIBE);
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
                logger.warn("",e);
            }

            return false;
        }

        @Override
        public boolean nextIndex() {
            try {
                if(!this.connection.isOpen()) {
                    if(index(this.index)) {
                        return nextIndex();
                    } else {
                        return false;
                    }
                }

                if(!this.connection.read(this.readBuffer, ChronicleTcp.HEADER_SIZE + 8)) {
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
                    if(!this.connection.read(this.readBuffer, excerptSize)) {
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
}
