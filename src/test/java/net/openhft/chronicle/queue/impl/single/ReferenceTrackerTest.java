package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ReferenceTrackerTest {
    @Rule
    public final TemporaryFolder tmpDir = new TemporaryFolder();
    private final ReferenceTracker.ReverseCharSequenceIntegerEncoder encoder = new ReferenceTracker.ReverseCharSequenceIntegerEncoder();
    private TableStore tableStore;
    private ReferenceTracker tracker;

    private static String sequenceToString(final CharSequence sequence) {
        final StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < sequence.length(); i++) {
            buffer.append(sequence.charAt(i));
        }
        return buffer.toString();
    }

    @Before
    public void setUp() throws Exception {
        tableStore = SingleTableBuilder.binary(tmpDir.newFile("table-store.cq4t")).build();
        tracker = new ReferenceTracker(tableStore);
    }

    @Test
    public void shouldTrackReferencesCycles() throws IOException {
        tracker.acquired(17);
        tracker.acquired(17);

        assertReferenceCount(17, 2);

        tracker.acquired(17);

        assertReferenceCount(17, 3);

        tracker.released(17);
        tracker.released(17);

        assertReferenceCount(17, 1);
    }

    @Test
    public void shouldEncodeInteger() {
        for (int i = 0; i < 1_000; i++) {
            assertEncoding(ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
        }
    }

    @Test
    public void shouldEncodeZero() {
        assertEncoding(0);
    }

    private void assertEncoding(final int value) {
        encoder.encode(value);
        assertThat(sequenceToString(encoder), is(Integer.toString(value)));
    }

    private void assertReferenceCount(final int cycle, final long expectedCount) {
        assertThat(tableStore.acquireValueFor(Integer.toString(cycle)).getVolatileValue(), is(expectedCount));
    }
}