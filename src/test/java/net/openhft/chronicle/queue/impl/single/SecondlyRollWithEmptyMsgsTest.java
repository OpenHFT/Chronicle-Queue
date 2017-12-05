package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;

public class SecondlyRollWithEmptyMsgsTest {


    private static final String HELLO_WORLD = "Hello World";

    @Rule
    public final TestName testName = new TestName();

    @NotNull
    protected File getTmpDir() {
        return DirectoryUtils.tempDir(testName.getMethodName());
    }

    /**
     * tests that 2 empty messages don't cause a problem with the tailer
     * @throws InterruptedException  from thread.sleep
     */
    @Test
    public void test() throws InterruptedException {

        for (int i = 0; i < 2; i++) {
           SingleChronicleQueueBuilder.binary(getTmpDir()).rollCycle(RollCycles.TEST_SECONDLY).build();
            Thread.sleep(1_001);
        }


        SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).rollCycle(RollCycles.TEST_SECONDLY).build();

        q.acquireAppender().writeText(HELLO_WORLD);

        ExcerptTailer excerptTailer = q.createTailer().toStart();

        Assert.assertEquals(HELLO_WORLD, excerptTailer.readText());

    }
}
