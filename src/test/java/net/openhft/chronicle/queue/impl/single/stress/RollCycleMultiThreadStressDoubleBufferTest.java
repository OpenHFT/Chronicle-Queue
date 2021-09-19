package net.openhft.chronicle.queue.impl.single.stress;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.internal.main.InternalDumpMain;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueIn;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;

@Ignore("double buffering is turned off currently")
public class RollCycleMultiThreadStressDoubleBufferTest extends RollCycleMultiThreadStressTest {

    private AtomicBoolean queueDumped = new AtomicBoolean(false);

    public RollCycleMultiThreadStressDoubleBufferTest() {
        super(StressTestType.DOUBLEBUFFER);
    }

    @Test
    public void stress() throws Exception {
        super.stress();
    }

    @Before
    public void setUp() {
        queueDumped = new AtomicBoolean(false);
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressDoubleBufferTest().stress();
    }

    @Override
    protected ReaderCheckingStrategy getReaderCheckingStrategy() {
        return new DoubleBufferReaderCheckingStrategy(queueDumped);
    }

    /**
     * When double-buffering is enabled, we need to be more lenient in our
     * reader checks. The double buffering often means a writer's queue
     * entry doesn't necessarily get written in the same order as the
     * numbers were taken.
     * This checking strategy keeps track of values we've seen out of order
     * and ensures they get seen eventually. When the reader is complete the
     * sets of out-of-order values should be empty (i.e. accounted for).
     */
    class DoubleBufferReaderCheckingStrategy implements ReaderCheckingStrategy {

        private final AtomicBoolean queueDumped;
        private final HashSet<Integer> unexpectedValues = new HashSet<>();
        private final HashSet<Integer> skippedValue = new HashSet<>();
        private int outOfOrderCount = 0;

        DoubleBufferReaderCheckingStrategy(AtomicBoolean queueDumped) {
            this.queueDumped = queueDumped;
        }

        @Override
        public void checkDocument(DocumentContext dc, ExcerptTailer tailer, RollingChronicleQueue queue,
                                  int lastTailerCycle, int lastQueueCycle, int expected, ValueIn valueIn) {
            for (int i = 0; i < NUMBER_OF_INTS; i++) {
                int v = valueIn.int32();
                if (i == 0 && v != expected) {
                    if (!unexpectedValues.remove(expected)) {
                        skippedValue.add(expected);
                    }
                    if (!skippedValue.remove(v)) {
                        unexpectedValues.add(v);
                    }
                    outOfOrderCount++;
                }
            }
        }

        @Override
        public void postReadCheck(RollingChronicleQueue queue) {
            Jvm.debug().on(getClass(), "Out-of-order count: " + outOfOrderCount);
            if (skippedValue.size() > 0 || unexpectedValues.size() > 0) {
                Jvm.error().on(getClass(), "Skipped " + skippedValue + ", Unexpected " + unexpectedValues);
                if (DUMP_QUEUE && !queueDumped.getAndSet(true)) {
                    InternalDumpMain.dump(queue.file(), System.out, Long.MAX_VALUE);
                }
            }
        }
    }
}
