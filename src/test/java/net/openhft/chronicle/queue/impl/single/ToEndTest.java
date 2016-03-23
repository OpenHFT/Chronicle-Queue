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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by daniel on 07/03/2016.
 */
public class ToEndTest {
    @Test
    public void toEndTest() {
        String baseDir = OS.TARGET + "/toEndTest-" + System.nanoTime();
        System.out.println(baseDir);
        IOTools.shallowDeleteDirWithFiles(baseDir);
        List<Integer> results = new ArrayList<>();
        RollingChronicleQueue queue = new SingleChronicleQueueBuilder(baseDir)
                .indexCount(8)
                .indexSpacing(1)
                .build();

        checkOneFile(baseDir);
        ExcerptAppender appender = queue.createAppender();
        checkOneFile(baseDir);

        for (int i = 0; i < 10; i++) {
            final int j = i;
            appender.writeDocument(wire -> wire.write(() -> "msg").int32(j));
        }

        checkOneFile(baseDir);

        ExcerptTailer tailer = queue.createTailer();
        checkOneFile(baseDir);

        tailer.toEnd();
        assertEquals(10, queue.rollCycle().toSequenceNumber(tailer.index()));
        checkOneFile(baseDir);
        fillResults(tailer, results);
        checkOneFile(baseDir);
        assertEquals(0, results.size());

        tailer.toStart();
        checkOneFile(baseDir);
        fillResults(tailer, results);
        assertEquals(10, results.size());
        checkOneFile(baseDir);

        IOTools.shallowDeleteDirWithFiles(baseDir);
    }

    @Test
    public void toEndBeforeWriteTest() {
        String baseDir = OS.TARGET + "/toEndBeforeWriteTest";
        IOTools.shallowDeleteDirWithFiles(baseDir);

        ChronicleQueue queue = new SingleChronicleQueueBuilder(baseDir).build();
        checkOneFile(baseDir);

        // if this appender isn't created, the tailer toEnd doesn't cause a roll.
        ExcerptAppender appender = queue.createAppender();
        checkOneFile(baseDir);

        ExcerptTailer tailer = queue.createTailer();
        checkOneFile(baseDir);

        ExcerptTailer tailer2 = queue.createTailer();
        checkOneFile(baseDir);

        tailer.toEnd();
        checkOneFile(baseDir);

        tailer2.toEnd();
        checkOneFile(baseDir);

        /*for (int i = 0; i < 10; i++) {
            final int j = i;
            appender.writeDocument(wire -> wire.write(() -> "msg").int32(j));
        }*/
        IOTools.shallowDeleteDirWithFiles(baseDir);
    }

    private void checkOneFile(String baseDir) {
        String[] files = new File(baseDir).list();

        if (files == null || files.length == 0)
            return;

        if (files.length == 1)
            assertTrue(files[0], files[0].startsWith("2"));
        else
            fail("Too many files " + Arrays.toString(files));
    }

    @NotNull
    private List<Integer> fillResults(ExcerptTailer tailer, List<Integer> results) {
        for (int i = 0; i < 10; i++) {
            if (!tailer.readDocument(wire -> results.add(wire.read(() -> "msg").int32())))
                break;
        }
        return results;
    }
}
