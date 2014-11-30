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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class SinkTcp extends TcpConnection {
    protected final Logger logger;
    protected final String name;
    protected final AtomicBoolean running;
    protected final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;

    protected SinkTcp(String name, final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder) {
        this.builder = builder;
        this.name = ChronicleTcp.connectionName(name, this.builder.bindAddress(), this.builder.connectAddress());
        this.logger = LoggerFactory.getLogger(this.name);
        this.running = new AtomicBoolean(false);
    }

    @Override
    public String toString() {
        return this.name;
    }

    public void open() throws IOException {
        close();
        running.set(true);

        SocketChannel socketChannel = openSocketChannel();
        if(socketChannel != null) {
            socketChannel.configureBlocking(false);
            socketChannel.socket().setTcpNoDelay(true);
            socketChannel.socket().setReceiveBufferSize(this.builder.receiveBufferSize());
            socketChannel.socket().setSoTimeout(0);
            socketChannel.socket().setSoLinger(false, 0);

            super.setSocketChannel(socketChannel);
        }

        running.set(false);
    }

    public void close() throws IOException {
        this.running.set(false);
        super.close();
    }

    public abstract boolean isLocalhost();
    protected abstract SocketChannel openSocketChannel() throws IOException;
}
