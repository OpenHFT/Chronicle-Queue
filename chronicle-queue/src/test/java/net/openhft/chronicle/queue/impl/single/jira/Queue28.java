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
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class Queue28 extends ChronicleQueueTestBase {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {WireType.TEXT},
                {WireType.BINARY}
        });
    }

    private final WireType wireType;

    public Queue28(WireType wireType) {
        this.wireType = wireType;
    }

    /*
     * Tailer doesn't work if created before the appender
     *
     * See https://higherfrequencytrading.atlassian.net/browse/QUEUE-28
     */

    @Test
    public void test() throws IOException {
        final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build();

        final ExcerptTailer tailer = queue.createTailer();
        assertFalse(tailer.readDocument(r -> r.read(TestKey.test).int32()));

        final ExcerptAppender appender = queue.createAppender();
        appender.writeDocument(w -> w.write(TestKey.test).int32(1));

        assertTrue(tailer.readDocument(r -> r.read(TestKey.test).int32()));
    }
}