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
import java.net.NetworkInterface;
import java.nio.channels.SocketChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class SourceTcpInitiator extends SourceTcp {
    private SocketChannel socketChannel;

    public SourceTcpInitiator(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder) {
        super(
            "source-acceptor",
            builder,
            new ThreadPoolExecutor(
                1,
                1,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>())
        );

        this.socketChannel = null;
    }

    protected Runnable createHandler() {
        return new Runnable() {
            @Override
            public void run() {
                while (running.get()) {
                    socketChannel = null;
                    while (running.get() && socketChannel == null) {
                        try {
                            socketChannel = SocketChannel.open();
                            socketChannel.configureBlocking(true);

                            if (builder.bindAddress() != null) {
                                socketChannel.bind(builder.bindAddress());
                            }

                            socketChannel.connect(builder.connectAddress());

                            logger.info("Connected to {} from {}", socketChannel.getRemoteAddress(), socketChannel.getLocalAddress());
                        } catch (IOException e) {
                            logger.info("Failed to connect to {}, retrying", builder.connectAddress());

                            try {
                                Thread.sleep(builder.reconnectionIntervalMillis());
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }

                            socketChannel = null;
                        }
                    }

                    if (socketChannel != null) {
                        try {
                            createSessionHandler(socketChannel).run();
                        } catch (IOException e) {
                            logger.error("Failed to create session handler.", e);
                        }
                    }
                }
            }
        };
    }

    @Override
    public boolean isLocalhost() {
        if(builder.connectAddress().getAddress().isLoopbackAddress()) {
            return true;
        }

        try {
            return NetworkInterface.getByInetAddress(builder.connectAddress().getAddress()) != null;
        } catch (Exception ignored) {
        }

        return false;
    }
}
