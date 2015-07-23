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
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.Jvm;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.*;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class ChronicleTcpTestBase {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ChronicleTcpTestBase.class);

    protected static final boolean TRACE_TEST_EXECUTION = Boolean.getBoolean("openhft.traceTestExecution");

    @Rule
    public final TestName testName = new TestName();
    @Rule
    public final TemporaryFolder folder= new TemporaryFolder(new File(System.getProperty("java.io.tmpdir")));
    @Rule
    public final ErrorCollector errorCollector = new ErrorCollector();

    private final SecureRandom random = new SecureRandom();

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            if(TRACE_TEST_EXECUTION) {
                LOGGER.info("Starting test: {}.{}",
                    description.getClassName(),
                    description.getMethodName()
                );
            }
        }
    };

    // *************************************************************************
    //
    // *************************************************************************

    protected String getTmpDir(String name) {
        try {
            String mn = testName.getMethodName();
            File path = mn == null
                ? folder.newFolder(getClass().getSimpleName(), name)
                : folder.newFolder(getClass().getSimpleName(), mn, name);

            LOGGER.debug("tmpdir: " + path);

            return path.getAbsolutePath();
        } catch(IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected String getTmpDir(String prefix, String name) {
        try {
            String mn = testName.getMethodName();
            File path = mn == null
                    ? folder.newFolder(getClass().getSimpleName(), prefix, name)
                    : folder.newFolder(getClass().getSimpleName(), mn, prefix, name);

            LOGGER.debug("tmpdir: " + path);

            return path.getAbsolutePath();
        } catch(IOException e) {
            throw new IllegalStateException(e);
        }
    }

    synchronized String getVanillaTestPath() {
        return getVanillaTestPath("vanilla-" + Math.abs(random.nextLong()));
    }

    synchronized String getVanillaTestPath(String suffix) {
        return getTmpDir(suffix);
    }

    synchronized String getVanillaTestPath(String prefix, String suffix) {
        return getTmpDir(prefix, suffix);
    }

    protected synchronized String getIndexedTestPath() {
       final String path = getTmpDir("indexed-" + Math.abs(random.nextLong()));
        ChronicleTools.deleteOnExit(path);
        return path;
    }

    protected synchronized String getIndexedTestPath(String suffix) {
        final String path = getTmpDir(suffix);
        ChronicleTools.deleteOnExit(path);

        return path;
    }

    protected synchronized String getIndexedTestPath(String prefix, String suffix) {
        final String path = getTmpDir(prefix, suffix);
        ChronicleTools.deleteOnExit(path);

        return path;
    }

    // *************************************************************************
    //
    // *************************************************************************

    protected final class PortSupplier extends TcpConnectionHandler {
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
        public void onError(SelectableChannel channel, Exception exception) {
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

        public int getAndAssertOnError() {
            final int port = port();
            assertNotEquals(-1, port);

            return port;
        }
    }

    protected static void assertIndexedClean(String path) {
        assertNotNull(path);
        File index = new File(path + ".index");
        File data = new File(path + ".data");

        if(index.exists()) {
            Assert.assertTrue(index.delete());
        }
        if(data.exists()) {
            Assert.assertTrue(data.delete());
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    public void testNonBlockingClient(final Chronicle source, final Chronicle sink, final long timeout) throws IOException, InterruptedException  {
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
                            long first = tailer.readLong();
                            long second = tailer.readLong();
                            assertEquals(i, first);
                            assertEquals(i + 1, second);
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
