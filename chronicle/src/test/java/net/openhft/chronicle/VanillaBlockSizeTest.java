package net.openhft.chronicle;

import org.junit.Test;

import java.io.IOException;
import java.util.Random;

/**
 * @author luke
 *         Date: 4/7/15
 */
public class VanillaBlockSizeTest {
    @Test
    public void testMaxSize() throws IOException {
        final int SAMPLE = 10000000;
        final int ITEM_SIZE = 256;
        byte[][] byteArrays;
        Random random = new Random();

        Chronicle chronicle = ChronicleQueueBuilder.vanilla("blocksizetest")
                .dataBlockSize(Integer.MAX_VALUE)
                .indexBlockSize(Integer.MAX_VALUE)
                .build();
        ExcerptAppender appender = chronicle.createAppender();

        System.out.println("Allocating...");
        byteArrays = new byte[SAMPLE][ITEM_SIZE];

        // Randomize
        System.out.println("Randomize...");
        for (int i=0;i<SAMPLE;i++) {
            random.nextBytes(byteArrays[0]);
        }

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
        int count = 0;
        while (true) {
            if (index >= SAMPLE) {
                index = 0;
            }
            long start = System.nanoTime();
            appender.startExcerpt(ITEM_SIZE);
            appender.write(byteArrays[index], 0, ITEM_SIZE);
            appender.finish();

            if (count % 100000000 == 0) {
                System.out.println(count+" written");
            }

            index++;
            count++;
        }
    }

    public static void main(String[] args) throws IOException {
        new VanillaBlockSizeTest().testMaxSize();
    }
}
