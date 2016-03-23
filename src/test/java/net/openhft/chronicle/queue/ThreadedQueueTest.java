/*
 *
 *  *     Copyright (C) ${YEAR}  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * @author Rob Austin.
 */
public class ThreadedQueueTest {

    public static final int MESSAGE_SIZE = 1024;
    public static final int REQUIRED_COUNT = 10;
    private static final int BLOCK_SIZE = 256 << 20;

    @Test(timeout = 10000)
    public void testMultipleThreads() throws java.io.IOException, InterruptedException, ExecutionException, TimeoutException {

        final String path = ChronicleQueueTestBase.getTmpDir() + "/deleteme.q";

        new File(path).deleteOnExit();

        final AtomicInteger counter = new AtomicInteger();

        ExecutorService tailerES = Executors.newSingleThreadExecutor(/*new NamedThreadFactory("tailer", true)*/);
        Future tf = tailerES.submit(() -> {
            try {
                final ChronicleQueue rqueue = new SingleChronicleQueueBuilder(path)
                        .wireType(WireType.BINARY)
                        .blockSize(BLOCK_SIZE)
                        .build();

                final ExcerptTailer tailer = rqueue.createTailer();
                final Bytes bytes = Bytes.elasticByteBuffer();

                while (counter.get() < REQUIRED_COUNT && !Thread.interrupted()) {
                    bytes.clear();
                    if (tailer.readBytes(bytes))
                        counter.incrementAndGet();
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        });

        ExecutorService appenderES = Executors.newSingleThreadExecutor(/*new NamedThreadFactory("appender", true)*/);
        Future af = appenderES.submit(() -> {
            try {
                final ChronicleQueue wqueue = new SingleChronicleQueueBuilder(path)
                        .wireType(WireType.BINARY)
                        .blockSize(BLOCK_SIZE)
                        .build();

                final ExcerptAppender appender = wqueue.createAppender();

                final Bytes message = Bytes.elasticByteBuffer();
                for (int i = 0; i < REQUIRED_COUNT; i++) {
                    message.clear();
                    message.append(i);
                    appender.writeBytes(message);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        });

        appenderES.shutdown();
        tailerES.shutdown();

        long end = System.currentTimeMillis() + 9000;
        af.get(9000, TimeUnit.MILLISECONDS);
        tf.get(end - System.currentTimeMillis(), TimeUnit.MILLISECONDS);

        assertEquals(REQUIRED_COUNT, counter.get());
    }

    @Test//(timeout = 5000)
    public void testTailerReadingEmptyQueue() throws java.io.IOException {

        final String path = ChronicleQueueTestBase.getTmpDir() + "/deleteme.q";

        new File(path).deleteOnExit();

        final AtomicInteger counter = new AtomicInteger();
        final ChronicleQueue rqueue = new SingleChronicleQueueBuilder(path)
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(BLOCK_SIZE)
                .build();

        final ExcerptTailer tailer = rqueue.createTailer();

        final ChronicleQueue wqueue = new SingleChronicleQueueBuilder(path)
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(BLOCK_SIZE)
                .build();

        Bytes bytes = Bytes.elasticByteBuffer();
        assertFalse(tailer.readBytes(bytes));

        final ExcerptAppender appender = wqueue.createAppender();
        appender.writeBytes(Bytes.wrapForRead("Hello World".getBytes()));

        bytes.clear();
        assertTrue(tailer.readBytes(bytes));
        assertEquals("Hello World", bytes.toString());


    }

}
