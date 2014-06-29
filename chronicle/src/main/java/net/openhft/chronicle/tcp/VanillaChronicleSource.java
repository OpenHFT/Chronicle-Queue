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

import net.openhft.chronicle.VanillaChronicle;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

/**
 * A Chronicle as a service to be replicated to any number of clients.
 * Clients can restart from where ever they are up to.
 *
 * <p>Can be used an in process component which wraps the underlying Chronicle
 * and offers lower overhead than using ChronicleSource
 *
 * @author peter.lawrey
 */
public class VanillaChronicleSource extends ChronicleSource {
    private final VanillaChronicle vanillaChronicle;
    private final Logger logger;

    public VanillaChronicleSource(@NotNull VanillaChronicle chronicle, int port) throws IOException {
        super(chronicle, ChronicleSourceConfig.DEFAULT, new InetSocketAddress(port));

        this.vanillaChronicle = chronicle;
        this.logger = LoggerFactory.getLogger(getClass().getName() + "." + name());
    }

    public void checkCounts(int min, int max) {
        vanillaChronicle.checkCounts(min, max);
    }

    @Override
    protected Runnable createSocketHandler(SocketChannel channel) throws IOException {
        return new Handler(channel);
    }

    // *************************************************************************
    //
    // *************************************************************************

    private final class Handler extends AbstractSocketHandler {
        private boolean nextIndex;

        public Handler(@NotNull SocketChannel socket) throws IOException {
            super(socket);
            this.nextIndex = true;
        }

        @Override
        public void onSelectResult(final Set<SelectionKey> keys) throws IOException {
            final Iterator<SelectionKey> it = keys.iterator();

            while (it.hasNext()) {
                final SelectionKey key = it.next();
                it.remove();

                if(key.isReadable()) {
                    try {
                        this.index = readIndex(socket);
                        if(this.index == -1) {
                            this.nextIndex = true;
                            this.tailer = tailer.toStart();
                            this.index = tailer.index();
                        } else if(this.index == -2) {
                            this.nextIndex = false;
                            this.tailer = tailer.toEnd();
                            this.index = tailer.index();
                        } else {
                            this.nextIndex = false;
                        }

                        buffer.clear();
                        buffer.putInt(SYNC_IDX_LEN);
                        buffer.putLong(this.index);
                        buffer.flip();
                        TcpUtil.writeAll(socket, buffer);

                        setLastHeartbeatTime();

                        key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                        keys.clear();
                        break;
                    } catch(EOFException e) {
                        key.selector().close();
                        throw e;
                    }
                } else if(key.isWritable()) {
                    final long now = System.currentTimeMillis();
                    if(!closed && !publishData()) {
                        if (lastHeartbeatTime <= now) {
                            buffer.clear();
                            buffer.putInt(IN_SYNC_LEN);
                            buffer.putLong(0L);
                            buffer.flip();
                            TcpUtil.writeAll(socket, buffer);
                            setLastHeartbeatTime(now);
                        }
                    }
                }
            }
        }

        private boolean publishData() throws IOException {
            if(nextIndex) {
                if (!tailer.nextIndex()) {
                    pause();
                    if (!closed && !tailer.nextIndex()) {
                        return false;
                    }
                }
            } else {
                if(!tailer.index(this.index)) {
                    return false;
                } else {
                    this.nextIndex = true;
                }
            }

            pauseReset();

            final long size = tailer.capacity();
            long remaining = size + TcpUtil.HEADER_SIZE;

            buffer.clear();
            buffer.putInt((int) size);
            buffer.putLong(tailer.index());

            // for large objects send one at a time.
            if (size > buffer.capacity() / 2) {
                while (remaining > 0) {
                    int size2 = (int) Math.min(remaining, buffer.capacity());
                    buffer.limit(size2);
                    tailer.read(buffer);
                    buffer.flip();
                    remaining -= buffer.remaining();
                    TcpUtil.writeAll(socket, buffer);
                }
            } else {
                buffer.limit((int) remaining);
                tailer.read(buffer);
                int count = 1;
                while (count++ < MAX_MESSAGE) {
                    if (tailer.nextIndex()) {
                        if (tailer.wasPadding()) {
                            throw new AssertionError("Entry should not be padding - remove");
                        }

                        if (tailer.capacity() + TcpUtil.HEADER_SIZE >= buffer.capacity() - buffer.position()) {
                            break;
                        }

                        // if there is free space, copy another one.
                        int size2 = (int) tailer.capacity();
                        buffer.limit(buffer.position() + size2 + TcpUtil.HEADER_SIZE);
                        buffer.putInt(size2);
                        buffer.putLong(tailer.index());
                        tailer.read(buffer);
                    }
                }

                buffer.flip();
                TcpUtil.writeAll(socket, buffer);
            }

            if (buffer.remaining() > 0) {
                throw new EOFException("Failed to send index=" + tailer.index());
            }

            return true;
        }
    }
}