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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleType;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class InMemoryChronicleSink extends AbstractChronicleSink {
    private final ChronicleType type;

    public InMemoryChronicleSink(final ChronicleType type, String hostName, int port) {
        super(hostName, port);

        this.type = type;
    }

    public InMemoryChronicleSink(final ChronicleType type, String hostName, int port, @NotNull ChronicleSinkConfig config) {
        super(hostName, port, config);

        this.type = type;
    }

    public InMemoryChronicleSink(final ChronicleType type, @NotNull InetSocketAddress address) {
        super(address);

        this.type = type;
    }

    public InMemoryChronicleSink(final ChronicleType type, @NotNull InetSocketAddress address, @NotNull ChronicleSinkConfig config) {
        super(address, config);

        this.type = type;
    }

    @Override
    public ExcerptTailer createTailer() throws IOException {
        return this.type == ChronicleType.INDEXED
            ? new InMemoryIndexedExcerptTailer(super.newConnector())
            : new InMemoryVanillaExcerptTailer(super.newConnector());
    }

    // *************************************************************************
    //
    // *************************************************************************

    private abstract class AbstractInMemoryExcerptTailer extends NativeBytes implements ExcerptTailer {
        protected final Logger logger;
        protected final SynkConnector connector;
        protected final ByteBuffer buffer;
        protected boolean firstMessage;
        protected long index;
        protected int lastSize;

        public AbstractInMemoryExcerptTailer(@NotNull final SynkConnector connector) {
            super(NO_PAGE, NO_PAGE);

            this.connector = connector;
            this.buffer = connector.buffer();
            this.firstMessage = true;
            this.index = -1;
            this.lastSize = 0;
            this.logger = LoggerFactory.getLogger(getClass().getName() + "@" + connector.remoteAddress().toString());
        }

        @Override
        public boolean index(long index) {
            if(!connector.isOpen()) {
                this.index = index;
                this.lastSize = 0;

                if(!connector.isOpen()) {
                    connector.open();
                }

                return connector.write(ByteBuffer.allocate(8).putLong(0,this.index));
            }

            throw new UnsupportedOperationException();
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
            try {
                connector.close();
            } catch (IOException ioe) {
                logger.warn("IOException",ioe);
            }
        }

        @Override
        public ExcerptTailer toStart() {
            index(-1);
            return this;
        }

        @Override
        public ExcerptTailer toEnd() {
            throw new UnsupportedOperationException();
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
            return 0;
        }

        @Override
        public Chronicle chronicle() {
            return InMemoryChronicleSink.this;
        }
    }

    private final class InMemoryIndexedExcerptTailer extends AbstractInMemoryExcerptTailer {
        public InMemoryIndexedExcerptTailer(@NotNull final SynkConnector connector) {
            super(connector);

            this.startAddr = ((DirectBuffer) buffer).address();
            this.capacityAddr = startAddr + buffer.capacity();
        }

        @Override
        public boolean nextIndex() {
            try {
                if(!connector.isOpen()) {
                    index(this.index);
                }

                if(!connector.readIfNeeded(
                    !firstMessage ? 4 : TcpUtil.HEADER_SIZE ,
                    !firstMessage ? TcpUtil.HEADER_SIZE : TcpUtil.HEADER_SIZE + 8)) {
                    return false;
                }

                if (firstMessage) {
                    long receivedIndex = buffer.getLong();
                    if (this.index == -1 && receivedIndex != 0) {
                        if (receivedIndex != index) {
                            throw new StreamCorruptedException("Expected index " + index + " but got " + receivedIndex);
                        }
                    }

                    index = receivedIndex;
                    this.firstMessage = false;
                }

                int excerptSize = buffer.getInt();
                switch (excerptSize) {
                    case InProcessChronicleSource.IN_SYNC_LEN:
                    case InProcessChronicleSource.PADDED_LEN:
                        this.index++; //TODO: increment index on padded entry ?
                        return false;
                    default:
                        break;
                }

                if (excerptSize > 128 << 20 || excerptSize < 0) {
                    throw new StreamCorruptedException("Size was " + excerptSize);
                }

                if(buffer.remaining() < excerptSize) {
                    if(!connector.read(buffer.remaining() - excerptSize)) {
                        return false;
                    }
                }

                positionAddr = startAddr + buffer.position();
                limitAddr = startAddr + buffer.limit();
                lastSize = excerptSize;
                index++;
            } catch (IOException e) {
                try {
                    connector.close();
                } catch (IOException ioe) {
                    logger.warn("IOException",ioe);
                }

                return false;
            }

            return true;
        }
    }

    private final class InMemoryVanillaExcerptTailer extends AbstractInMemoryExcerptTailer {
        public InMemoryVanillaExcerptTailer(@NotNull final SynkConnector connector) {
            super(connector);


            this.startAddr = ((DirectBuffer) buffer).address();
            this.capacityAddr = startAddr + buffer.capacity();
        }

        @Override
        public boolean nextIndex() {
            try {
                if(!connector.isOpen()) {
                    index(this.index);
                }

                if(!connector.readIfNeeded(TcpUtil.HEADER_SIZE + 8, TcpUtil.HEADER_SIZE + 8 + 8)) {
                    return false;
                }

                long excerptIndex = buffer.getLong();
                int excerptSize = buffer.getInt();
                if(excerptSize == InProcessChronicleSource.IN_SYNC_LEN) {
                    return false;
                }

                if (excerptSize > 128 << 20 || excerptSize < 0) {
                    throw new StreamCorruptedException("Size was " + excerptSize);
                }

                if(buffer.remaining() < excerptSize) {
                    if(!connector.read(buffer.remaining() - excerptSize)) {
                        return false;
                    }
                }

                positionAddr = startAddr + buffer.position();
                limitAddr = startAddr + buffer.limit();
                lastSize = excerptSize;
                index = excerptIndex;
            } catch (IOException e) {
                try {
                    connector.close();
                } catch (IOException ioe) {
                    logger.warn("IOException",ioe);
                }

                return false;
            }

            return true;
        }
    }
}
