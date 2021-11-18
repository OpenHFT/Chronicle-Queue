package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.main.DumpMain;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static junit.framework.TestCase.fail;
import static net.openhft.chronicle.queue.RollCycles.HOURLY;
import static net.openhft.chronicle.queue.RollCycles.TEST4_DAILY;
import static org.junit.Assume.assumeTrue;

/**
 * check that method writes are thread safe when used with queue.methodWriter
 */
@RunWith(Parameterized.class)
public class TestMethodWriterWithThreads extends ChronicleQueueTestBase {

    private static final int AMEND = 1;
    private static final int CREATE = 2;
    @Rule
    public final TestName testName = new TestName();
    private ThreadLocal<Amend> amendTL = ThreadLocal.withInitial(Amend::new);
    private ThreadLocal<Create> createTL = ThreadLocal.withInitial(Create::new);
    private I methodWriter;
    private AtomicBoolean fail = new AtomicBoolean();
    private boolean doubleBuffer;

    public TestMethodWriterWithThreads(boolean doubleBuffer) {
        this.doubleBuffer = doubleBuffer;
    }

    @Parameterized.Parameters(name = "doubleBuffer={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[]{true}, new Object[]{false});
    }

    @Before
    public void check64bit() {
        assumeTrue(Jvm.is64bit());
    }

    @Test
    public void test() throws FileNotFoundException {

        File tmpDir = getTmpDir();
        try (final ChronicleQueue q = builder(tmpDir, WireType.BINARY).rollCycle(HOURLY).doubleBuffer(doubleBuffer).build()) {

            methodWriter = q.methodWriter(I.class);

            IntStream.range(0, 1000)
                    .parallel()
                    .forEach(i -> {
                        final ExcerptTailer tailer = q.createTailer();
                        try {
                            creates();
                            amends();
                            final MethodReader methodReader = tailer.methodReader(newReader());
                            for (int j = 0; j < 2 && !fail.get(); )
                                if (methodReader.readOne())
                                    j++;
                        } finally {
                            // close appender acquired by creates above
                            Closeable.closeQuietly(q.acquireAppender(), tailer);
                        }
                        if (fail.get())
                            fail();
                    });

        } finally {
            if (fail.get()) {
                DumpMain.dump(tmpDir.getAbsolutePath());
            }
        }
    }

    @NotNull
    private I newReader() {
        return new I() {

            @Override
            public void amend(final Amend amend) {
                if (amend.type != AMEND) {
                    fail.set(true);
                    fail("amend type=" + amend.type);
                }
            }

            @Override
            public void create(final Create create) {
                if (create.type != CREATE) {
                    fail.set(true);
                    fail("create type=" + create.type);
                }
            }
        };
    }

    private void amends() {
        methodWriter.amend(amendTL.get().type(AMEND));
    }

    private void creates() {
        methodWriter.create(createTL.get().type(CREATE));
    }

    @NotNull
    protected SingleChronicleQueueBuilder builder(@NotNull File file, @NotNull WireType wireType) {
        return SingleChronicleQueueBuilder.builder(file, wireType).rollCycle(TEST4_DAILY).testBlockSize();
    }

    interface I {
        void amend(Amend q);

        void create(Create q);
    }

    static class Amend extends SelfDescribingMarshallable {
        int type;

        Amend type(final int type) {
            this.type = type;
            return this;
        }
    }

    static class Create extends SelfDescribingMarshallable {
        int type;

        Create type(final int type) {
            this.type = type;
            return this;
        }
    }
}
