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

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.io.IOTools;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertTrue;
import static net.openhft.chronicle.ChronicleQueueBuilder.remoteTailer;
import static net.openhft.chronicle.ChronicleQueueBuilder.vanilla;
import static net.openhft.chronicle.ChronicleQueueBuilder.ReplicaChronicleQueueBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

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

     static String getTmpDir() {
        return System.getProperty("java.io.tmpdir");
    }

    synchronized String getVanillaTestPath(String suffix) {
        final String path = getTmpDir() + "/" + "chronicle-" + testName
                .getMethodName() + suffix;
        final File f = new File(path);
        if (f.exists()) {
            f.delete();
        }

        return path;
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

        public void reset() {
            this.port.set(-1);
        }

        public int port() {
            if (port.get() == -1) {
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

    // *************************************************************************
    //
    // *************************************************************************

    public void testNonBlockingClient(final Chronicle source, final Chronicle sink, final long timeout) throws Exception {
        final int messages = 1000;

        final Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                AffinityLock lock = AffinityLock.acquireLock();

                try {
                    final ExcerptAppender appender = source.createAppender();
                    final Random random = new Random();

                    for (int i = 1; i <= messages; i++) {
                        appender.startExcerpt(16);
                        appender.writeLong(i);
                        appender.writeLong(i + 1);
                        appender.finish();

                        Thread.sleep(random.nextInt(10));
                    }

                    appender.close();
                    LOGGER.info("Finished writing messages");

                } catch (AssertionError e) {
                    errorCollector.addError(e);
                } catch (Exception e) {
                    errorCollector.addError(e);
                } finally {
                    lock.release();
                }
            }
        });

        final Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                AffinityLock lock = AffinityLock.acquireLock();

                try {
                    long start = 0;
                    long end = 0;
                    long noNextIndex = 0;
                    boolean hasNext = false;

                    final ExcerptTailer tailer = sink.createTailer();
                    for(int i = 1; i <= messages; ) {
                        start   = System.currentTimeMillis();
                        hasNext = tailer.nextIndex();
                        end     = System.currentTimeMillis();

                        assertTrue("Timeout exceeded " + (end - start), (end - start) < timeout);

                        if(hasNext) {
                            assertEquals(i, tailer.readLong());
                            assertEquals(i + 1, tailer.readLong());
                            i++;

                            tailer.finish();
                        } else {
                            noNextIndex++;
                        }
                    }

                    tailer.close();

                    LOGGER.info("There where {} nextIndex without data", noNextIndex);
                } catch (AssertionError e) {
                    errorCollector.addError(e);
                } catch (Exception e) {
                    errorCollector.addError(e);
                } finally {
                    lock.release();
                }
            }
        });

        t1.start();
        t2.start();
        t1.join();
        t2.join();

        source.close();
        source.clear();

        sink.close();
        sink.clear();
    }
}
