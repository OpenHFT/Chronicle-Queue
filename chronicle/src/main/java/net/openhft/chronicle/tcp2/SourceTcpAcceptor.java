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

import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.lang.thread.NamedThreadFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class SourceTcpAcceptor extends SourceTcp {

    private final ThreadPoolExecutor executor;

    public SourceTcpAcceptor(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder) {
        super("source-acceptor", builder);

        this.executor = new ThreadPoolExecutor(
            builder.acceptorDefaultThreads() + 1,
            Math.max(builder.acceptorMaxThreads(), builder.acceptorMaxThreads() + 1),
            builder.acceptorThreadPoolkeepAliveTime(),
            builder.acceptorThreadPoolkeepAliveTimeUnit(),
            new SynchronousQueue<Runnable>(),
            new NamedThreadFactory("chronicle-source", true),
            new ServerSessionRejectedHandler());
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Override
    public boolean open() throws IOException {
        this.running.set(true);
        this.executor.execute(new Handler());

        return this.running.get();
    }

    @Override
    public boolean close()  throws IOException {
        running.set(false);
        executor.shutdown();

        try {
            executor.awaitTermination(
                builder.selectTimeout() * 2,
                builder.selectTimeoutUnit());
        } catch(InterruptedException e) {
            // Ignored
        }

        return !running.get();
    }

    // *************************************************************************
    //
    // *************************************************************************

    private class ServerSessionRejectedHandler implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(final Runnable r, final ThreadPoolExecutor executor) {
            logger.info("No room for a new sessions <{}>", r);
        }
    }

    private class Handler implements Runnable {

        @Override
        public void run() {
            try {
                final Selector selector = Selector.open();

                final ServerSocketChannel server = ServerSocketChannel.open();
                server.socket().setReuseAddress(true);
                server.socket().bind(builder.bindAddress(), builder.acceptorMaxBacklog());
                server.configureBlocking(false);
                server.register(selector, SelectionKey.OP_ACCEPT);

                while(running.get()) {
                    if (selector.select(builder.selectTimeoutMillis()) > 0) {
                        final Set<SelectionKey> keys = selector.selectedKeys();
                        for (final SelectionKey key : keys) {
                            if (key.isAcceptable()) {
                                SocketChannel channel = server.accept();
                                logger.info("Accepted connection from: {}",  channel.getRemoteAddress());

                                executor.execute(createServerSessionHandler(channel));
                            }
                        }

                        keys.clear();
                    } else {
                        logger.info("No incoming connections on {}, wait", builder.bindAddress());
                    }
                }

                selector.close();
                server.close();
            } catch(IOException e) {
                logger.warn("", e);
            }
        }
    }
}
