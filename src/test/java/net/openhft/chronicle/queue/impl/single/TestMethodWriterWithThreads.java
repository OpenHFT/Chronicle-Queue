/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.main.DumpMain;
import net.openhft.chronicle.threads.NamedThreadFactory;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.TestCase.fail;
import static net.openhft.chronicle.queue.impl.single.ThreadLocalAppender.acquireThreadLocalAppender;
import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.HOURLY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_DAILY;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * check that method writes are thread safe when used with queue.methodWriter
 */
@RunWith(Parameterized.class)
public class TestMethodWriterWithThreads extends QueueTestCommon {

    private static final int AMEND = 1;
    private static final int CREATE = 2;
    @Rule
    public final TestName testName = new TestName();
    private ThreadLocal<Amend> amendTL = ThreadLocal.withInitial(Amend::new);
    private ThreadLocal<Create> createTL = ThreadLocal.withInitial(Create::new);
    private I methodWriter;
    private AtomicBoolean fail = new AtomicBoolean();
    private boolean doubleBuffer;
    private volatile ExecutorService workers;
    private volatile ChronicleQueue queue;
    private File tmpDir;

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

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Test(timeout = 30_000)
    public void test() throws Exception {
        tmpDir = getTmpDir();
        queue = builder(tmpDir, WireType.BINARY).rollCycle(HOURLY).doubleBuffer(doubleBuffer).build();
        methodWriter = queue.methodWriter(I.class);
        workers = Executors.newFixedThreadPool(8, new NamedThreadFactory("method-writer"));
        List<Future<?>> results = new ArrayList<>();
        for (int i = 0; i < 1000; i++)
            results.add(workers.submit(this::writeAndRead));
        workers.shutdown();
        for (Future<?> result : results)
            result.get();
    }

    private void writeAndRead() {
        try (final ExcerptTailer tailer = queue.createTailer()) {
            creates();
            amends();
            final MethodReader methodReader = tailer.methodReader(newReader());
            for (int j = 0; j < 2 && !fail.get(); ) {
                if (Thread.currentThread().isInterrupted())
                    throw new AssertionError("Method-reader worker interrupted");
                if (methodReader.readOne())
                    j++;
            }
        } finally {
            // close appender acquired by creates above
            Closeable.closeQuietly(acquireThreadLocalAppender(queue));
        }
        if (fail.get())
            fail();
    }

    @Override
    protected void preAfter() {
        // JUnit timeouts can start teardown while the test body is still unwinding.
        // Stop owned workers before closing their Queue or checking global references.
        try {
            if (workers != null) {
                workers.shutdownNow();
                assertTrue("Method-writer workers did not stop", workers.awaitTermination(5, TimeUnit.SECONDS));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while stopping method-writer workers", e);
        } finally {
            Closeable.closeQuietly(queue);
        }
        if (fail.get()) {
            try {
                DumpMain.dump(tmpDir.getAbsolutePath());
            } catch (FileNotFoundException e) {
                throw new AssertionError("Unable to dump failed method-writer queue", e);
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
    private SingleChronicleQueueBuilder builder(@NotNull File file, @NotNull WireType wireType) {
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
