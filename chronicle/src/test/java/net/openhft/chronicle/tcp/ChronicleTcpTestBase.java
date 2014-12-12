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

import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ChronicleTcpTestBase {

    protected class PortSupplier extends TcpConnectionHandler {
        private AtomicInteger port;
        private CountDownLatch latch;

        public PortSupplier() {
            this.port = new AtomicInteger(-1);
            this.latch = new CountDownLatch(1);
        }

        @Override
        public void onServerSocketStarted(final ServerSocketChannel channel) {
            this.port.set(channel.socket().getLocalPort());
            this.latch.countDown();
        }

        public int port() {
            if(port.get() == -1) {
                try {
                    this.latch.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                }
            }

            return this.port.get();
        }
    }
}
