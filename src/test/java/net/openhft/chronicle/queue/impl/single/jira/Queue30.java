/*
 *
 *    Copyright (C) 2015  higherfrequencytrading.com
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU Lesser General Public License as published by
 *    the Free Software Foundation, either version 3 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Lesser General Public License for more details.
 *
 *    You should have received a copy of the GNU Lesser General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package net.openhft.chronicle.queue.impl.single.jira;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

/**
 * See https://higherfrequencytrading.atlassian.net/browse/QUEUE-30
 */
public class Queue30 extends ChronicleQueueTestBase {

    @Ignore
    @Test
    public void testMT() throws IOException, InterruptedException {
        final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(WireType.TEXT)
                .blockSize(640_000)
                .build();

        Throwable[] tref = {null};
        Runnable r = () -> {
            try {
                final String name = Thread.currentThread().getName();
                final ExcerptAppender appender = queue.createAppender();
                for (int count = 0; ; count++) {
                    final int c = count;
                    appender.writeDocument(w ->
                            w.write(() -> "thread").text(name)
                                    .write(() -> "count").int32(c)
                    );

                    if (count % 10_000 == 0) {
                        LOGGER.info(name + "> " + count);
                    }
                }
            } catch (Throwable t) {
                tref[0] = t;
            }
        };

        Thread t1 = new Thread(r);
        Thread t2 = new Thread(r);

        t1.start();
        t2.start();

        t1.join();
        t2.join();
        if (tref != null)
            throw new AssertionError(tref[0]);

    }

    @Ignore
    @Test
    public void testST() throws IOException {
        final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(WireType.TEXT)
                .blockSize(640_000)
                .build();

        final String name = Thread.currentThread().getName();
        final ExcerptAppender appender = queue.createAppender();
        for (int count = 0; ; count++) {
            final int c = count;
            appender.writeDocument(w ->
                    w.write(() -> "thread").text(name)
                            .write(() -> "count").int32(c)
            );

            if (count % 10_000 == 0) {
                LOGGER.info(name + "> " + count);
            }
        }
    }
}
