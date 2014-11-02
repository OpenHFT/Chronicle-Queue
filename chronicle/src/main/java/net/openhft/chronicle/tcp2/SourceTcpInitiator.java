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

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.*;

public final class SourceTcpInitiator extends SourceTcp {

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
    }

    protected Runnable createHandler() {
        return new Runnable() {
            @Override
            public void run() {
                while (running.get()) {
                    SocketChannel channel = null;
                    for (int i = 0; i < builder.maxOpenAttempts() && running.get() && channel == null; i++) {
                        try {
                            channel = SocketChannel.open();
                            channel.configureBlocking(true);

                            if (builder.bindAddress() != null) {
                                channel.bind(builder.bindAddress());
                            }

                            channel.connect(builder.connectAddress());

                            logger.info("Connected to {} from {}", channel.getRemoteAddress(), channel.getLocalAddress());
                        } catch (IOException e) {
                            logger.info("Failed to connect to {}, retrying", builder.connectAddress());

                            try {
                                Thread.sleep(builder.reconnectTimeoutMillis());
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }

                            channel = null;
                        }
                    }

                    if (channel != null) {
                        createSessionHandler(channel).run();
                    }
                }
            }
        };
    }
}
