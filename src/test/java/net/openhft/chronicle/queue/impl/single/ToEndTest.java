package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

/**
 * Created by daniel on 07/03/2016.
 */
public class ToEndTest {
    @Test
    public void toEndTest(){
        System.out.println(OS.TARGET + "/test");
        IOTools.shallowDeleteDirWithFiles(OS.TARGET + "/test");
        List<Integer> results = new ArrayList<>();
        ChronicleQueue queue = new SingleChronicleQueueBuilder(OS.TARGET + "/test").
        bufferCapacity(4 << 20)
                .buffered(false)
                .wireType(WireType.BINARY)
                .blockSize(64 << 20)
                .build();

        ExcerptAppender appender = queue.createAppender();


        for(int i=0; i<10; i++) {
            final int j=i;
            appender.writeDocument(wire -> wire.write(() -> "msg").int32(j));
        }

        ExcerptTailer tailer = queue.createTailer();

        tailer.toEnd();
        fillResults(tailer, results);
        assertEquals(0, results.size());

        tailer.toStart();
        fillResults(tailer, results);
        assertEquals(10, results.size());
    }

    @NotNull
    private List<Integer> fillResults(ExcerptTailer tailer,List<Integer> results) {
        for(int i=0; i<10; i++) {
            tailer.readDocument(wire -> results.add(wire.read(() -> "msg").int32()));
        }
        return results;
    }
}
