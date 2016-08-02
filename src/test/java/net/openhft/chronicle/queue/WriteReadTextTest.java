package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by peter on 02/08/16.
 */
public class WriteReadTextTest {
    @Test
    public void writeReadText() {
        String myPath = OS.TMP + "/writeReadText-" + System.nanoTime();
        try (SingleChronicleQueue theQueue = ChronicleQueueBuilder.single(myPath).build()) {
            ExcerptAppender appender = theQueue.createAppender();
            ExcerptTailer tailer = theQueue.createTailer();

            String mySourceString = "[\"upd\",\"cme_ilink\"," +
                    "[[1469743199691,1469743199691]," +
                    "[\"GSJOMMO\",\"GSJOMMO\"]," +
                    "[321,401]," +
                    "[\"\",\"\"]]]";

            appender.writeText(mySourceString);
            StringBuilder myDestinationStringBuilder = new StringBuilder();
            tailer.readText(myDestinationStringBuilder);

            Assert.assertEquals(mySourceString, myDestinationStringBuilder.toString());
        }
    }
}
