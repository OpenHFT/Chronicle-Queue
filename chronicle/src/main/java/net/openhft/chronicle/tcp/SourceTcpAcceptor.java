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
import net.openhft.lang.thread.NamedThreadFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

public final class SourceTcpAcceptor extends SourceTcp {

    public SourceTcpAcceptor(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder) {
        super(
                "source-acceptor",
                builder,
                new ThreadPoolExecutor(
                        builder.acceptorDefaultThreads() + 1,
                        Math.max(builder.acceptorMaxThreads(), builder.acceptorMaxThreads() + 1),
                        builder.acceptorThreadPoolkeepAliveTime(),
                        builder.acceptorThreadPoolkeepAliveTimeUnit(),
                        new SynchronousQueue<Runnable>(),
                        new NamedThreadFactory("chronicle-source", true))
        );
    }


    protected Runnable createHandler() {
        return new Runnable() {
            @Override
            public void run() {
                ServerSocketChannel socketChannel = null;

                try {

                    socketChannel = ServerSocketChannel.open();
                    socketChannel.socket().setReuseAddress(true);
                    socketChannel.socket().bind(builder.bindAddress(), builder.acceptorMaxBacklog());
                    socketChannel.configureBlocking(false);

                    builder.connectionListener().onListen(socketChannel);

                    final VanillaSelector selector = new VanillaSelector()
                            .open()
                            .register(socketChannel, SelectionKey.OP_ACCEPT, new Attached());

                    final long selectTimeout = builder.selectTimeout();
                    final VanillaSelectionKeySet selectionKeys = selector.vanillaSelectionKeys();

                    while (running.get()) {
                        int nbKeys = selector.select(0, selectTimeout);

                        if (nbKeys > 0) {
                            if (selectionKeys != null) {
                                final SelectionKey[] keys = selectionKeys.keys();
                                final int size = selectionKeys.size();

                                for (int k = 0; k < size; k++) {
                                    final SelectionKey key = keys[k];
                                    if (key != null) {
                                        if (key.isAcceptable()) {
                                            SocketChannel channel = socketChannel.accept();
                                            logger.info("Accepted connection from: {}", channel.getRemoteAddress());

                                            executor.execute(createSessionHandler(channel));
                                        }
                                    }
                                }

                                selectionKeys.clear();
                            } else {
                                final Set<SelectionKey> keys = selector.selectionKeys();

                                for (final SelectionKey key : keys) {
                                    if (key.isAcceptable()) {
                                        SocketChannel channel = socketChannel.accept();
                                        logger.info("Accepted connection from: {}", channel.getRemoteAddress());

                                        executor.execute(createSessionHandler(channel));
                                    }
                                }

                                keys.clear();
                            }
                        }
                    }

                    selector.close();
                    socketChannel.close();
                } catch (IOException e) {
                    builder.connectionListener().onError(socketChannel, e);
                    logger.warn("", e);
                }
            }
        };
    }

    @Override
    public boolean isLocalhost() {
        return true;
    }
}
