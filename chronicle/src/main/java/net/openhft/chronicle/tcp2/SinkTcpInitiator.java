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
import java.net.NetworkInterface;
import java.nio.channels.SocketChannel;

public class SinkTcpInitiator extends SinkTcp {
    public SinkTcpInitiator(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder) {
        super("sink-initiator", builder);
    }

    @Override
    public SocketChannel openSocketChannel() throws IOException {
        SocketChannel channel = null;
        for (int i = 0; i < this.builder.maxOpenAttempts() && this.running.get() && channel == null; i++) {
            try {
                channel = SocketChannel.open();
                channel.configureBlocking(true);

                if(this.builder.bindAddress() != null) {
                    channel.bind(this.builder.bindAddress());
                }

                channel.connect(this.builder.connectAddress());

                logger.info("Connected to {} from {}", channel.getRemoteAddress(), channel.getLocalAddress());
            } catch(IOException e) {
                logger.info("Failed to connect to {}, retrying", this.builder.connectAddress());

                try {
                    Thread.sleep(this.builder.reconnectTimeoutMillis());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }

                channel = null;
            }
        }

        return channel;
    }

    @Override
    public boolean isLocalhost() {
        if(this.builder.connectAddress().getAddress().isLoopbackAddress()) {
            return true;
        }

        try {
            return NetworkInterface.getByInetAddress(this.builder.connectAddress().getAddress()) != null;
        } catch(Exception e)  {
        }

        return false;
    }

}
