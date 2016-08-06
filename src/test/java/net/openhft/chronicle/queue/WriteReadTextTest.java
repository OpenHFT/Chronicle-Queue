package net.openhft.chronicle.queue;

import org.junit.Assert;
import org.junit.Test;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

/**
 * Created by peter on 02/08/16.
 */
public class WriteReadTextTest {

    private static final String CONSTRUCTED = "[\"abc\",\"comm_link\"," +
            "[[1469743199691,1469743199691]," +
            "[\"ABCDEFXH\",\"ABCDEFXH\"]," +
            "[321,456]," +
            "[\"\",\"\"]]]";
    private static final String MINIMAL = "[\"abc\"]";
    private static final String REALISTIC = "[\"abc\",\"comm_link\",[[1469743199691,1469743199691],[\"ABCDEFXH\",\"ABCDEFXH\"],[321,456],[-1408156298,-841885387],[12345,9876],[-841885387,-1408156298],[9876,12345],[0,0],[\"FIX.4.2\",\"FIX.4.2\"],[243,324],[\"NewOrderSingle\",\"ExecutionReport\"],[12862,13622],[\"Q1W2E3R4T5Y6U7I8O9P0\",\"ABC\"],[\"ABCDEFXH\",\"X\"],[1469743199686,1469743199691],[\"ABC\",\"Q1W2E3R4T5Y6U7I8O9P0\"],[\"X\",\"ABCDEFXH\"],[\"RU,IT\",\"\"],[13621,12862],[\"76537\",\"76537\"],[\"12345\",\"12345\"],[\"AUTOMATED_EXECUTION_ORDER_PRIVATE_NO_BROKER_INTERVENTION\",\"\"],[\"\",\"683895170272\"],[10,10],[\"LIMIT\",\"LIMIT\"],[\"\",\"0\"],[473100.0,473100.0],[\"SELL\",\"SELL\"],[\"NQ\",\"NQ\"],[\"DAY\",\"DAY\"],[1469743199686,1469743199691],[\"IJK123\",\"IJK123\"],[\"FUTURE\",\"FUTURE\"],[\"CRUTOMER\",\"\"],[true,true],[false,false],[\"\",\"\"],[\"\",\"\"],[\"NFY_9\",\"\"],[\"12345\",\"12345\"],\"\",[31,55],[\"\",\"RU,IT\"],[\"NaN\",0.0],[-2147483648,0],[\"\",\"68250:27217624\"],[\"\",\"NEW\"],[\"\",\"NEW\"],[-2147483648,2563],[\"\",\"NEW\"],[-2147483648,10],[null,1469750400000],[-2147483648,-2147483648],[\"NaN\",\"NaN\"],[-2147483648,-2147483648],[null,null],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],\"\",[-2147483648,-2147483648],[\"\",\"\"],[\"\",\"\"],[-2147483648,-2147483648],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],[false,false],[-2147483648,-2147483648],[-2147483648,-2147483648],[-2147483648,-2147483648],[null,null],[-2147483648,-2147483648],[\"\",\"\"],[\"NaN\",\"NaN\"],[-2147483648,-2147483648],[\"\",\"\"],[-2147483648,-2147483648],[\"\",\"\"],[\"\",\"\"]]]";

    @Test
    public void testMinimalCase() {
        doTest(MINIMAL);
    }

    @Test
    public void testConstructedCase() {
        doTest(CONSTRUCTED);
    }

    @Test
    public void testRealisticCase() {
        doTest(REALISTIC);
    }

    private void doTest(String problematic) {

        String myPath = OS.TMP + "/writeReadText-" + System.nanoTime();

        try (SingleChronicleQueue theQueue = ChronicleQueueBuilder.single(myPath).build()) {

            ExcerptAppender appender = theQueue.acquireAppender();
            ExcerptTailer tailer = theQueue.createTailer();

            appender.writeText(problematic);

            StringBuilder tmpReadback = new StringBuilder();

            tailer.readText(tmpReadback);

            Assert.assertEquals(problematic, tmpReadback.toString());
        }
    }

}
