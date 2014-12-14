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

import org.junit.Rule;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertNotEquals;

public class ChronicleTcpTestBase {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ChronicleTcpTestBase.class);

    @Rule
    public final TestName testName = new TestName();

    @Rule
    public final ErrorCollector errorCollector = new ErrorCollector();

    // *************************************************************************
    //
    // *************************************************************************

    protected synchronized String getTestName() {
        return testName.getMethodName();
    }

    protected synchronized String getTmpDir() {
        return System.getProperty("java.io.tmpdir");
    }

    // *************************************************************************
    //
    // *************************************************************************

    protected final class PortSupplier implements TcpConnectionListener {
        private final AtomicInteger port;
        private final CountDownLatch latch;

        public PortSupplier() {
            this.port = new AtomicInteger(-1);
            this.latch = new CountDownLatch(1);
        }

        @Override
        public void onListen(final ServerSocketChannel channel) {
            this.port.set(channel.socket().getLocalPort());
            this.latch.countDown();
        }

        @Override
        public void onError(SelectableChannel channel, IOException exception) {
            errorCollector.addError(exception);

            this.port.set(-1);
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

        public int getAndCheckPort() {
            final int port = port();
            assertNotEquals(-1, port);

            LOGGER.info("{} : listening on port {}", getTestName(), port);

            return port;
        }
    }
}
