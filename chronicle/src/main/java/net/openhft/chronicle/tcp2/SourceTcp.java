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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class SourceTcp {
    protected final Logger logger;
    protected final String name;
    protected final AtomicBoolean running;
    protected final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;
    protected final ThreadPoolExecutor executor;

    protected SourceTcp(String name, final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, ThreadPoolExecutor executor) {
        this.builder = builder;
        this.name = ChronicleTcp2.connectionName(name, this.builder.bindAddress(), this.builder.connectAddress());
        this.logger = LoggerFactory.getLogger(this.name);
        this.running = new AtomicBoolean(false);
        this.executor = executor;
    }

    public boolean open() throws IOException {
        this.running.set(true);
        this.executor.execute(createHandler());

        return this.running.get();
    }

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

    @Override
    public String toString() {
        return this.name;
    }

    protected abstract Runnable createHandler();

    /**
     * Creates a session handler according to the Chronicle the sources is connected to.
     *
     * @param socketChannel     The {@link java.nio.channels.SocketChannel}
     * @return                  The ServerSessionHandler
     */
    protected Runnable createSessionHandler(final @NotNull SocketChannel socketChannel) {
        final Chronicle chronicle = builder.chronicle();
        if(chronicle != null) {
            if(chronicle instanceof IndexedChronicle) {
                return new IndexedServerSessionHandler(socketChannel);
            } else if(chronicle instanceof VanillaChronicle) {
                return new VanillaServerSessionHandler(socketChannel);
            } else {
                throw new IllegalStateException("Chronicle must be Indexed or Vanilla");
            }
        }

        throw new IllegalStateException("Chronicle can't be null");
    }

    /**
     * Abstract class for Indexed and Vanilla chronicle replicaton
     */
    private abstract class ServerSessionHandler implements Runnable, Closeable {
        private final SocketChannel socketChannel;

        private ServerSessionHandler(final @NotNull SocketChannel socketChannel) {
            this.socketChannel = socketChannel;

        }

        @Override
        public void close() throws IOException {
            if(this.socketChannel.isOpen()) {
                this.socketChannel.close();
            }
        }
    }

    /**
     * IndexedChronicle session handler
     */
    private class IndexedServerSessionHandler extends ServerSessionHandler {
        private IndexedServerSessionHandler(final @NotNull SocketChannel socketChannel) {
            super(socketChannel);
        }

        @Override
        public void run() {
        }
    }

    /**
     * VanillaChronicle session handler
     */
    private class VanillaServerSessionHandler extends ServerSessionHandler {
        private VanillaServerSessionHandler(final @NotNull SocketChannel socketChannel) {
            super(socketChannel);
        }

        @Override
        public void run() {
        }
    }
}
