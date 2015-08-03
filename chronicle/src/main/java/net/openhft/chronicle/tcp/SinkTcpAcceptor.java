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

import net.openhft.chronicle.ChronicleQueueBuilder;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

public class SinkTcpAcceptor extends SinkTcp {
    private ServerSocketChannel socketChannel;
    private VanillaSelector selector;

    public SinkTcpAcceptor(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, boolean blocking) {
        super("sink-acceptor", builder, blocking);

        this.socketChannel = null;
        this.selector = null;
    }

    @Override
    public SocketChannel openSocketChannel() throws IOException {
        if(this.socketChannel == null ) {
            this.socketChannel = ServerSocketChannel.open();
            this.socketChannel.socket().setReuseAddress(true);
            this.socketChannel.socket().bind(builder.bindAddress());
            this.socketChannel.configureBlocking(false);

            this.selector = new VanillaSelector();
            this.selector.open();
            this.selector.register(socketChannel, SelectionKey.OP_ACCEPT, new Attached());
        }

        final long selectTimeout = builder.selectTimeout();
        final VanillaSelectionKeySet selectionKeys = selector.vanillaSelectionKeys();

        SocketChannel channel = null;
        int nbKeys = selector.select(0, selectTimeout);
        if (nbKeys > 0) {
            if (selectionKeys != null) {
                final SelectionKey[] keys = selectionKeys.keys();
                final int size = selectionKeys.size();

                for (int k = 0; k < size; k++) {
                    final SelectionKey key = keys[k];
                    if (key != null) {
                        if (key.isAcceptable()) {
                            channel = socketChannel.accept();
                            logger.info("Accepted connection from: " + channel.getRemoteAddress());
                        }
                    }
                }

                selectionKeys.clear();

            } else {
                final Set<SelectionKey> keys = selector.selectionKeys();

                for (final SelectionKey key : keys) {
                    if (key.isAcceptable()) {
                        channel = socketChannel.accept();
                        logger.info("Accepted connection from: " + channel.getRemoteAddress());
                    }
                }

                keys.clear();
            }
        }

        selector.close();
        selector = null;
        socketChannel.close();
        socketChannel = null;

        return channel;
    }

    @Override
    public boolean isLocalhost() {
        return true;
    }
}
