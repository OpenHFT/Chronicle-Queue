/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.WireUtil;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SingleChronicleQueueTest extends ChronicleQueueTestBase {

    @Test
    public void testHeaderCreation() throws IOException {
        SingleChronicleQueueFormat.from(SingleChronicleQueueBuilder.text(getTmpFile()));
    }

    @Test
    public void testAppendViaFormat() throws IOException {
        final SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.text(getTmpFile());
        final SingleChronicleQueueFormat format = SingleChronicleQueueFormat.from(builder);

        for(int i=0; i<10; i++) {
            final int n = i;
            format.append(w -> w.write(TestKey.test).text("event " + n));
        }
    }

    @Test
    public void testAppendAndReadViaFormat() throws IOException {
        final SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.text(getTmpFile());
        final SingleChronicleQueueFormat format = SingleChronicleQueueFormat.from(builder);

        for(int i=0; i<10; i++) {
            final int n = i;
            assertEquals(n, format.append(w -> w.write(TestKey.test).int32(n)));
        }

        long position = format.dataPosition();
        for(int i=0; i<10; i++) {
            final int n = i;
            position = format.read(position, r -> assertEquals(n, r.read(TestKey.test).int32()));
            assertTrue(WireUtil.NO_DATA != position);
        }
    }

    @Test
    public void testAppendViaExcerpts() throws IOException {
        final ChronicleQueue queue = SingleChronicleQueueBuilder.text(getTmpFile()).build();

        final ExcerptAppender appender = queue.createAppender();
        for(int i=0; i<10; i++) {
            final int n = i;
            assertEquals(n, appender.writeDocument(w -> w.write(TestKey.test).int32(n)));
        }
    }

    @Test
    public void testAppendAndReadViaExcerpts() throws IOException {
        final ChronicleQueue queue = SingleChronicleQueueBuilder.text(getTmpFile()).build();

        final ExcerptAppender appender = queue.createAppender();
        for(int i=0; i<10; i++) {
            final int n = i;
            assertEquals(n, appender.writeDocument(w -> w.write(TestKey.test).int32(n)));
            assertEquals(n, appender.lastWrittenIndex());
        }

        final ExcerptTailer tailer =queue.createTailer();
        for(int i=0; i<10; i++) {
            final int n = i;
            assertTrue(tailer.readDocument(r -> assertEquals(n, r.read(TestKey.test).int32())));
        }
    }

    enum TestKey implements WireKey {
        test
    }
}
