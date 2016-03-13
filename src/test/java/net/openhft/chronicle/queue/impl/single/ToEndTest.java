package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.WireType;
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
        String baseDir = OS.TARGET + "/toEndTest";
        System.out.println(baseDir);
        IOTools.shallowDeleteDirWithFiles(baseDir);
        List<Integer> results = new ArrayList<>();
        RollingChronicleQueue queue = new SingleChronicleQueueBuilder(baseDir)
                .wireType(WireType.BINARY)
                .blockSize(4 << 20)
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
