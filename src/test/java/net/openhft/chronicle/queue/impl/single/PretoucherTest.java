package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.NewChunkListener;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.stream.IntStream.range;
import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static net.openhft.chronicle.queue.impl.single.Pretoucher.PRETOUCHER_PREROLL_TIME_DEFAULT_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PretoucherTest {
    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    private final List<Integer> capturedCycles = new ArrayList<>();
    private final CapturingChunkListener chunkListener = new CapturingChunkListener();

    private static SingleChronicleQueue createQueue(final File path, final TimeProvider timeProvider) {
        return SingleChronicleQueueBuilder.
                binary(path).
                timeProvider(timeProvider).
                rollCycle(RollCycles.TEST_SECONDLY).
                testBlockSize().
                wireType(WireType.BINARY).
                build();
    }

    @Test
    public void shouldHandleCycleRoll() {
        File dir = tempDir("shouldHandleCycleRoll");
        try (final SingleChronicleQueue queue = createQueue(dir, clock::get);
             final Pretoucher pretoucher = new Pretoucher(createQueue(dir, clock::get), chunkListener, capturedCycles::add)) {

            range(0, 10).forEach(i -> {
                try (final DocumentContext ctx = queue.acquireAppender().writingDocument()) {
                    assertEquals(i, capturedCycles.size());
                    ctx.wire().write().int32(i);
                    ctx.wire().write().bytes(new byte[1024]);
                }
                try {
                    pretoucher.execute();
                } catch (InvalidEventHandlerException e) {
                    throw Jvm.rethrow(e);
                }
                assertEquals(i + 1, capturedCycles.size());
                try {
                    pretoucher.execute();
                } catch (InvalidEventHandlerException e) {
                    throw Jvm.rethrow(e);
                }
                assertEquals(i + 1, capturedCycles.size());
                clock.addAndGet(TimeUnit.SECONDS.toMillis(5L));
            });

            assertEquals(10, capturedCycles.size());
        }
    }

    @Test
    public void shouldHandleCycleRollByPretoucher() {
        cycleRollByPretoucher(0);
    }

    @Test
    public void shouldHandleEarlyCycleRollByPretoucher() throws IllegalAccessException {
        hackStaticFinal(Pretoucher.class, "EARLY_ACQUIRE_NEXT_CYCLE", true);
        hackStaticFinal(Pretoucher.class, "PRETOUCHER_PREROLL_TIME_MS", 100);
        try {
            cycleRollByPretoucher(100);
        } finally {
            hackStaticFinal(Pretoucher.class, "EARLY_ACQUIRE_NEXT_CYCLE", false);
            hackStaticFinal(Pretoucher.class, "PRETOUCHER_PREROLL_TIME_MS", PRETOUCHER_PREROLL_TIME_DEFAULT_MS);
        }
    }

    private void cycleRollByPretoucher(int earlyMillis) {
        File dir = tempDir("shouldHandleEarlyCycleRoll");
        clock.set(100);
        try (final SingleChronicleQueue queue = createQueue(dir, clock::get);
             final Pretoucher pretoucher = new Pretoucher(createQueue(dir, clock::get), chunkListener, capturedCycles::add)) {

            range(0, 10).forEach(i -> {
                try (final DocumentContext ctx = queue.acquireAppender().writingDocument()) {
                    assertEquals(i == 0 ? 0 : i + 1, capturedCycles.size());
                    ctx.wire().write().int32(i);

                    ctx.wire().write().bytes(new byte[1024]);
                }
                try {
                    pretoucher.execute();
                } catch (InvalidEventHandlerException e) {
                    throw Jvm.rethrow(e);
                }
                assertEquals(i + 1, capturedCycles.size());
                clock.addAndGet(950 - earlyMillis);
                try {
                    pretoucher.execute();
                } catch (InvalidEventHandlerException e) {
                    throw Jvm.rethrow(e);
                }
                clock.addAndGet(50 + earlyMillis);
                assertEquals(i + 2, capturedCycles.size());
            });

            assertEquals(11, capturedCycles.size());
            assertFalse(chunkListener.chunkMap.isEmpty());
        }
    }

    @Test
    public void dontWrite() throws IllegalAccessException {

        hackStaticFinal(Pretoucher.class, "CAN_WRITE", false);
        File dir = tempDir("shouldNotRoll");

        try (final SingleChronicleQueue queue = createQueue(dir, clock::get);
             final Pretoucher pretoucher = new Pretoucher(createQueue(dir, clock::get), chunkListener, capturedCycles::add)) {

            range(0, 10).forEach(i -> {
                try (final DocumentContext ctx = queue.acquireAppender().writingDocument()) {
                    assertEquals(i, capturedCycles.size());
                    ctx.wire().write().int32(i);
                    ctx.wire().write().bytes(new byte[1024]);
                }
                try {
                    pretoucher.execute();
                } catch (InvalidEventHandlerException e) {
                    throw Jvm.rethrow(e);
                }
                assertEquals(i + 1, capturedCycles.size());
                clock.addAndGet(TimeUnit.SECONDS.toMillis(5L));
                try {
                    pretoucher.execute();
                } catch (InvalidEventHandlerException e) {
                    throw Jvm.rethrow(e);
                }
                assertEquals(i + 1, capturedCycles.size());
            });

            assertEquals(10, capturedCycles.size());
        } finally {
            hackStaticFinal(Pretoucher.class, "CAN_WRITE", true);
        }
    }

    private void hackStaticFinal(Class<Pretoucher> pretoucherClass, String name, Object newValue) throws IllegalAccessException {
        // As junit has already initialised the class, and all tests are run in same classloader we need to hack the static variables.
        // Setting system properties wont't do it.
        // A better solution would be to run in separate classloaders but this was too much effort
        Field f = Jvm.getField(pretoucherClass, name);
        removeFinalModifier(f);
        f.set(null, newValue);
    }

    private void removeFinalModifier(Field f) throws IllegalAccessException {
        // yes this is the worst thing ever. Now wash your hands ;)
        Field modifiersField = Jvm.getField(Field.class, "modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(f, f.getModifiers() & ~Modifier.FINAL);
    }

    private static final class CapturingChunkListener implements NewChunkListener {
        private final TreeMap<String, List<Integer>> chunkMap = new TreeMap<>();

        @Override
        public void onNewChunk(final String filename, final int chunk, final long delayMicros) {
            chunkMap.computeIfAbsent(filename, f -> new ArrayList<>()).add(chunk);
        }
    }
}