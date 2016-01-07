package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
@RunWith(Parameterized.class)
public class IndexTest extends ChronicleQueueTestBase {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {WireType.TEXT},
                {WireType.BINARY}
        });
    }

    private final WireType wireType;

    /**
     * @param wireType
     */
    public IndexTest(WireType wireType) {
        this.wireType = wireType;
    }

    @Test
    public void test() throws IOException {

        final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build();

        final ExcerptAppender appender = queue.createAppender();
        final long cycle = appender.cycle();
        for (int i = 0; i < 5; i++) {
            final int n = i;
            assertEquals(ChronicleQueue.index(cycle, n), appender.writeDocument(w -> w.write
                    (ChronicleQueueTestBase.TestKey
                            .test).int32(n)));
            assertEquals(ChronicleQueue.index(cycle, n), appender.index());
        }
    }

}
