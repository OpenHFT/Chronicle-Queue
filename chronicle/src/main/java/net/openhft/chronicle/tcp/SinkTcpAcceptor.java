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
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

public class SinkTcpAcceptor extends SinkTcp {
    public SinkTcpAcceptor(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder) {
        super("sink-acceptor", builder);
    }

    @Override
    public SocketChannel openSocketChannel() throws IOException {
        final Selector selector = Selector.open();

        final ServerSocketChannel server = ServerSocketChannel.open();
        server.socket().setReuseAddress(true);
        server.socket().bind(builder.bindAddress());
        server.configureBlocking(false);
        server.register(selector, SelectionKey.OP_ACCEPT);

        SocketChannel channel = null;
        for (int i = 0; (i < builder.maxOpenAttempts() || -1 == builder.maxOpenAttempts())  && running.get() && channel == null; i++) {
            if(selector.select(builder.selectTimeoutMillis()) > 0) {
                final Set<SelectionKey> keys = selector.selectedKeys();
                for (final SelectionKey key : keys) {
                    if (key.isAcceptable()) {
                        channel = server.accept();
                        logger.info("Accepted connection from: " + channel.getRemoteAddress());
                        break;
                    }
                }

                keys.clear();
            } else {
                logger.info("No incoming connections on {}, wait", builder.bindAddress());
            }
        }

        selector.close();
        server.close();

        return channel;
    }

    @Override
    public boolean isLocalhost() {
        return true;
    }
}
