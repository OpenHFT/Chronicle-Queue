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

import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.WireUtil;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore
public class SingleChronicleQueueFormatTest extends ChronicleQueueTestBase {

    enum TestKey implements WireKey {
        test
    }

    int cycle() {
        return (int) (System.currentTimeMillis() / RollCycle.DAYS.length());
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testHeaderCreation() throws IOException {
        new SingleChronicleQueueFormat(
                SingleChronicleQueueBuilder.text(getTmpDir()),
                cycle(),
                DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now()))
            .buildHeader();
    }

    @Test
    public void testAppend() throws IOException {
        final SingleChronicleQueueFormat format =
            new SingleChronicleQueueFormat(
                SingleChronicleQueueBuilder.text(getTmpDir()),
                cycle(),
                DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now()))
            .buildHeader();

        for(int i=0; i<10; i++) {
            final int n = i;
            format.append(w -> w.write(TestKey.test).text("event " + n));
        }
    }

    @Test
    public void testAppendAndRead() throws IOException {
        final SingleChronicleQueueFormat format =
            new SingleChronicleQueueFormat(
                SingleChronicleQueueBuilder.text(getTmpDir()),
                cycle(),
                DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now()))
            .buildHeader();

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
}
