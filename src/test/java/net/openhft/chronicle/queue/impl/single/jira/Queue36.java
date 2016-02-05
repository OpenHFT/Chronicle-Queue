/**
 *     Copyright (C) 2016  higherfrequencytrading.com
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
package net.openhft.chronicle.queue.impl.single.jira;


import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * See https://higherfrequencytrading.atlassian.net/browse/QUEUE-36
 */
public class Queue36 extends ChronicleQueueTestBase {
    @Test
    public void testTail() throws IOException {
        File basePath = getTmpDir();
        ChronicleQueue queue = new SingleChronicleQueueBuilder(basePath)
            .wireType(WireType.TEXT)
            .build();

        ExcerptTailer tailer = queue.createTailer();
        tailer.toStart();

        assertFalse(tailer.readDocument(d -> {}));

        String[] fileNames = basePath.list();
        assertTrue(fileNames == null || fileNames.length == 0);
    }
}
