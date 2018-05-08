package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.DumpQueueMain;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;

public class StuckQueueTest {

    @Test
    public void test() throws FileNotFoundException {
        URL resource = StuckQueueTest.class.getResource("/stuck.queue.test/20180508-1249.cq4");
        File dir = new File(resource.getFile()).getParentFile();

        DumpQueueMain.dump(dir.getAbsolutePath());

        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(dir).rollCycle(RollCycles.MINUTELY).build()) {

            ExcerptTailer tailer = q.createTailer();

            int cycle = q.rollCycle().toCycle(0x18406e100000000L);
            WireStore wireStore = q.storeForCycle(cycle, q.epoch(), false);
            String absolutePath = wireStore.file().getAbsolutePath();
            System.out.println(absolutePath);
            Assert.assertTrue(absolutePath.endsWith("20180508-1249.cq4"));
            //   Assert.assertTrue(tailer.moveToIndex(0x18406e100000000L));


            try (DocumentContext dc = tailer.readingDocument()) {
                Assert.assertTrue(dc.isPresent());
                String actual = dc.wire().read("hello").text();
                Assert.assertEquals("world", actual);
                System.out.println(Long.toHexString(dc.index()));
            }
        }
    }

}

