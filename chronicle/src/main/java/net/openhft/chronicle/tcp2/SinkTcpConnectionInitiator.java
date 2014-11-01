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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.nio.channels.SocketChannel;

public class SinkTcpConnectionInitiator extends SinkTcpConnection {
    public SinkTcpConnectionInitiator(final InetSocketAddress connectAddress) {
        super("sink-initiator", null, connectAddress);
    }

    public SinkTcpConnectionInitiator(String name, final InetSocketAddress connectAddress) {
        super(name + "-sink-initiator", null, connectAddress);
    }

    public SinkTcpConnectionInitiator(final InetSocketAddress bindAddress, final InetSocketAddress connectAddress) {
        super("sink-initiator", bindAddress, connectAddress);
    }

    public SinkTcpConnectionInitiator(String name, final InetSocketAddress bindAddress, final InetSocketAddress connectAddress) {
        super(name + "-sink-initiator", bindAddress, connectAddress);
    }

    @Override
    public SocketChannel openSocketChannel() throws IOException {
        SocketChannel channel = null;
        for (int i = 0; i < maxOpenAttempts && this.running.get() && channel == null; i++) {
            try {
                channel = SocketChannel.open();
                channel.configureBlocking(true);

                if(bindAddress != null) {
                    channel.bind(bindAddress);
                }

                channel.connect(connectAddress);

                logger.info("Connected to " + channel.getRemoteAddress() + " from " + channel.getLocalAddress());
            } catch(IOException e) {
                logger.info("Failed to connect to {}, retrying", connectAddress);

                try {
                    Thread.sleep(reconnectTimeoutUnit.toMillis(reconnectTimeout));
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
        if(connectAddress.getAddress().isLoopbackAddress()) {
            return true;
        }

        try {
            return NetworkInterface.getByInetAddress(connectAddress.getAddress()) != null;
        } catch(Exception e)  {
        }

        return false;
    }

}
