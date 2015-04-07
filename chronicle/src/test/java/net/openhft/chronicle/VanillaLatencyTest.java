package net.openhft.chronicle;

import org.HdrHistogram.Histogram;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

/**
 * @author luke
 *         Date: 4/7/15
 */
public class VanillaLatencyTest {

    final int COUNT = 100000000;
    final int SAMPLE = 10000000;
    final int ITEM_SIZE = 256;

    @Test
    public void testIndexedVsVanilla() throws IOException {
        Random random = new Random();

        System.out.println("Allocating...");
        byte[][] byteArrays = new byte[SAMPLE][ITEM_SIZE];

        // Randomize
        System.out.println("Randomize...");
        for (int i=0;i<SAMPLE;i++) {
            random.nextBytes(byteArrays[0]);
        }

        Chronicle indexedChronicle = ChronicleQueueBuilder.indexed("indexedlatency").build();
        System.out.println("----------------------- INDEXED -----------------------");
        latencyTest(indexedChronicle, COUNT, byteArrays);
        indexedChronicle.close();

        Chronicle vanillaChronicle = ChronicleQueueBuilder.vanilla("vanillalatency").build();
        System.out.println("----------------------- VANILLA -----------------------");
        latencyTest(vanillaChronicle, COUNT, byteArrays);
        vanillaChronicle.close();
    }

    private void latencyTest(Chronicle chronicle, long count, final byte[][] byteArrays) throws IOException {
        Histogram histogram = new Histogram(10000000000L, 3);
        ExcerptAppender appender = chronicle.createAppender();

        System.out.println("GC...");
        System.gc();

        // Warmup
        System.out.println("Warmup...");
        for (int i=0;i<SAMPLE;i++) {
            appender.startExcerpt(ITEM_SIZE);
            appender.write(byteArrays[i], 0, ITEM_SIZE);
            appender.finish();
        }

        System.out.println("Test...");
        int index = 0;
        for (int i=0; i<count; i++) {
            if (index >= SAMPLE) {
                index = 0;
            }
            long start = System.nanoTime();
            appender.startExcerpt(ITEM_SIZE);
            appender.write(byteArrays[index], 0, ITEM_SIZE);
            appender.finish();
            long end = System.nanoTime();
            histogram.recordValue(end - start);

            if (i > 0 && i % 10000000 == 0) {
                System.out.println("Total: "+i+" Mean: "+histogram.getMean()+" Max: "+histogram.getMaxValue()+
                        " StdDev: "+histogram.getStdDeviation());
            }

            index++;
        }
    }

    public static void main(String[] args) throws IOException {
        new VanillaLatencyTest().testIndexedVsVanilla();
    }
}
