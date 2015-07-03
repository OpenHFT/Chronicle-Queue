package net.openhft.chronicle;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class IndexedChronicleMaxDataBlocksTest extends IndexedChronicleTestBase {

    @Test
    public void testMaxDataBlocks() throws IOException {
        final String basePath = getTestPath();

        final Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath)
                .useCheckedExcerpt(true)
                .dataBlockSize(8 * 1024) // 8k
                .maxDataBlocks(10) // 10 block .. shouldn't allow 81 messages
                .build();


        ExcerptAppender appender = chronicle.createAppender();

        byte [] message = new byte[1024];
        ThreadLocalRandom.current().nextBytes(message);

        boolean iseCaught = false;
        for (int i = 0; i < 81 ; i ++) {
            try {
                appender.startExcerpt(message.length);
            } catch (IllegalStateException e) {
                if (i < 70) {
                    Assert.fail("Unexpected IllegalStateException, shouldn't happen at such a low index"
                                    + "[" + i + "]"
                                    + " exception:" + e);
                }
                //else this is the expected case
                iseCaught = true;
                break;
            }
            appender.write(message);
            appender.finish();
        }

        if (!iseCaught) {
            Assert.fail("expected IndexChronicle to throw IllegalStateException when attempting to add "
                            + "more data than allowed");
        }
    }
}
