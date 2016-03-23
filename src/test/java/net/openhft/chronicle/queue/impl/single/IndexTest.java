/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
@RunWith(Parameterized.class)
public class IndexTest extends ChronicleQueueTestBase {

    private final WireType wireType;

    /**
     * @param wireType the type of the wire
     */
    public IndexTest(@NotNull WireType wireType) {
        this.wireType = wireType;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
//                {WireType.TEXT}, // TODO Add CAS to LongArrayReference.
                {WireType.BINARY}
        });
    }

    @Test
    public void test() throws IOException {

        final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build();

        final ExcerptAppender appender = queue.createAppender();
        final int cycle = appender.cycle();
        for (int i = 0; i < 5; i++) {
            final int n = i;
            long index0 = queue.rollCycle().toIndex(cycle, n);
            appender.writeDocument(w -> w.write(TestKey.test).int32(n));
            long indexA = appender.lastIndexAppended();
            accessHexEquals(index0, indexA);
        }
    }

    public void accessHexEquals(long index0, long indexA) {
        assertEquals(Long.toHexString(index0) + " != " + Long.toHexString(indexA), index0, indexA);
    }

}
