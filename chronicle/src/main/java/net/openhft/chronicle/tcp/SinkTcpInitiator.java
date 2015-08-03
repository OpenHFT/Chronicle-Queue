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
import java.nio.channels.SocketChannel;

public class SinkTcpInitiator extends SinkTcp {
    public SinkTcpInitiator(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, boolean blocking) {
        super("sink-initiator", builder, blocking);
    }

    @Override
    public SocketChannel openSocketChannel() throws IOException {
        try {
            SocketChannel channel = SocketChannel.open();

            if(builder.bindAddress() != null) {
                channel.bind(builder.bindAddress());
            }

            channel.connect(builder.connectAddress());

            logger.info("Connected to {} from {}", channel.getRemoteAddress(), channel.getLocalAddress());

            return channel;
        } catch(IOException e) {
        }

        return null;
    }

    @Override
    public boolean isLocalhost() {
        return ChronicleTcp.isLocalhost(builder.connectAddress());
    }
}
