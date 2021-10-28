/*
 * Copyright 2016-2020 chronicle.software
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assume.assumeTrue;

@RequiredForClient
public class WriteReadTextTest extends QueueTestCommon {

    private static final String CONSTRUCTED = "[\"abc\",\"comm_link\"," + "[[1469743199691,1469743199691],"
            + "[\"ABCDEFXH\",\"ABCDEFXH\"]," + "[321,456]," + "[\"\",\"\"]]]";
    @NotNull
    private static final String EXTREMELY_LARGE;
    private static final String MINIMAL = "[\"abc\"]";
    private static final String REALISTIC = "[\"abc\",\"comm_link\",[[1469743199691,1469743199691],[\"ABCDEFXH\",\"ABCDEFXH\"],[321,456],[-1408156298,-841885387],[12345,9876],[-841885387,-1408156298],[9876,12345],[0,0],[\"FIX.4.2\",\"FIX.4.2\"],[243,324],[\"NewOrderSingle\",\"ExecutionReport\"],[12862,13622],[\"Q1W2E3R4T5Y6U7I8O9P0\",\"ABC\"],[\"ABCDEFXH\",\"X\"],[1469743199686,1469743199691],[\"ABC\",\"Q1W2E3R4T5Y6U7I8O9P0\"],[\"X\",\"ABCDEFXH\"],[\"RU,IT\",\"\"],[13621,12862],[\"76537\",\"76537\"],[\"12345\",\"12345\"],[\"AUTOMATED_EXECUTION_ORDER_PRIVATE_NO_BROKER_INTERVENTION\",\"\"],[\"\",\"683895170272\"],[10,10],[\"LIMIT\",\"LIMIT\"],[\"\",\"0\"],[473100.0,473100.0],[\"SELL\",\"SELL\"],[\"NQ\",\"NQ\"],[\"DAY\",\"DAY\"],[1469743199686,1469743199691],[\"IJK123\",\"IJK123\"],[\"FUTURE\",\"FUTURE\"],[\"CRUTOMER\",\"\"],[true,true],[false,false],[\"\",\"\"],[\"\",\"\"],[\"NFY_9\",\"\"],[\"12345\",\"12345\"],\"\",[31,55],[\"\",\"RU,IT\"],[\"NaN\",0.0],[-2147483648,0],[\"\",\"68250:27217624\"],[\"\",\"NEW\"],[\"\",\"NEW\"],[-2147483648,2563],[\"\",\"NEW\"],[-2147483648,10],[null,1469750400000],[-2147483648,-2147483648],[\"NaN\",\"NaN\"],[-2147483648,-2147483648],[null,null],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],\"\",[-2147483648,-2147483648],[\"\",\"\"],[\"\",\"\"],[-2147483648,-2147483648],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],[\"\",\"\"],[false,false],[-2147483648,-2147483648],[-2147483648,-2147483648],[-2147483648,-2147483648],[null,null],[-2147483648,-2147483648],[\"\",\"\"],[\"NaN\",\"NaN\"],[-2147483648,-2147483648],[\"\",\"\"],[-2147483648,-2147483648],[\"\",\"\"],[\"\",\"\"]]]";

    static {

        int largest = 20_993_248;

        StringBuilder tmpSB = new StringBuilder(largest + 6);

        while (tmpSB.length() < largest)
            tmpSB.append("0123456789ABCDE\n");

        EXTREMELY_LARGE = tmpSB.toString();
    }

    @Test
    public void testConstructed() {
        doTest(CONSTRUCTED);
    }

    @Test
    public void testExtremelyLarge() {
        assumeTrue(Jvm.is64bit());
        doTest(EXTREMELY_LARGE);
    }

    @Test
    public void testMinimal() {
        doTest(MINIMAL);
    }

    @Test
    public void testRealistic() {
        doTest(REALISTIC);
    }

    private void doTest(@NotNull String... problematic) {

        String myPath = OS.getTarget() + "/writeReadText-" + Time.uniqueId();

        try (ChronicleQueue theQueue = SingleChronicleQueueBuilder
                .single(myPath)
                .blockSize(Maths.nextPower2(EXTREMELY_LARGE.length() * 4, 256 << 10))
                // .testBlockSize() not suitable as large message sizes.
                .build()) {

            ExcerptAppender appender = theQueue.acquireAppender();
            ExcerptTailer tailer = theQueue.createTailer();

            StringBuilder tmpReadback = new StringBuilder();

            // If the tests don't fail, try increasing the number of iterations
            // Setting it very high may give you a JVM crash
            final int tmpNumberOfIterations = 5;

            for (int l = 0; l < tmpNumberOfIterations; l++) {
                for (int p = 0; p < problematic.length; p++) {
                    appender.writeText(problematic[p]);
                }
                for (int p = 0; p < problematic.length; p++) {
                    tailer.readText(tmpReadback);
                    Assert.assertEquals("write/readText", problematic[p], tmpReadback.toString());
                }
            }

            for (int l = 0; l < tmpNumberOfIterations; l++) {
                for (int p = 0; p < problematic.length; p++) {
                    final String tmpText = problematic[p];
                    appender.writeDocument(writer -> writer.getValueOut().text(tmpText));

                    tailer.readDocument(reader -> reader.getValueIn().textTo(tmpReadback));
                    String actual = tmpReadback.toString();
                    Assert.assertEquals(problematic[p].length(), actual.length());
                    for (int i = 0; i < actual.length(); i += 1024)
                        Assert.assertEquals("i: " + i, problematic[p].substring(i, Math.min(actual.length(), i + 1024)), actual.substring(i, Math.min(actual.length(), i + 1024)));
                    Assert.assertEquals(problematic[p], actual);
                }
            }
        }
    }
}
