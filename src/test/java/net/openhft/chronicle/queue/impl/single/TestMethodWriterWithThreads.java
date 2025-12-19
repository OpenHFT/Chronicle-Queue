/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.main.DumpMain;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static net.openhft.chronicle.queue.impl.single.ThreadLocalAppender.acquireThreadLocalAppender;
import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.HOURLY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_DAILY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * check that method writes are thread safe when used with queue.methodWriter
 */
@ExtendWith(TestMethodWriterWithThreads.TestMethodWriterWithThreadsTemplateProvider.class)
@SuppressWarnings({"deprecation", "removal"})
public class TestMethodWriterWithThreads extends QueueTestCommon {

    private static final int AMEND = 1;
    private static final int CREATE = 2;
    private final ThreadLocal<Amend> amendTL = ThreadLocal.withInitial(Amend::new);
    private final ThreadLocal<Create> createTL = ThreadLocal.withInitial(Create::new);
    private I methodWriter;
    private final AtomicBoolean fail = new AtomicBoolean();
    private final boolean doubleBuffer;

    public TestMethodWriterWithThreads(boolean doubleBuffer) {
        this.doubleBuffer = doubleBuffer;
    }

    private static Stream<TestMethodWriterWithThreadsCase> cases() {
        return Stream.of(new TestMethodWriterWithThreadsCase(true), new TestMethodWriterWithThreadsCase(false));
    }

    private static final class TestMethodWriterWithThreadsCase {
        private final boolean doubleBuffer;

        private TestMethodWriterWithThreadsCase(boolean doubleBuffer) {
            this.doubleBuffer = doubleBuffer;
        }
    }

    static final class TestMethodWriterWithThreadsTemplateProvider implements TestTemplateInvocationContextProvider {
        @Override
        public boolean supportsTestTemplate(ExtensionContext context) {
            return true;
        }

        @Override
        public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
            return cases().map(TestMethodWriterWithThreadsInvocationContext::new);
        }
    }

    private static final class TestMethodWriterWithThreadsInvocationContext implements TestTemplateInvocationContext {
        private final TestMethodWriterWithThreadsCase testCase;

        private TestMethodWriterWithThreadsInvocationContext(TestMethodWriterWithThreadsCase testCase) {
            this.testCase = testCase;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return "doubleBuffer=" + testCase.doubleBuffer;
        }

        @Override
        public java.util.List<Extension> getAdditionalExtensions() {
            return Collections.singletonList(new TestMethodWriterWithThreadsParameterResolver(testCase));
        }
    }

    private static final class TestMethodWriterWithThreadsParameterResolver implements ParameterResolver {
        private final TestMethodWriterWithThreadsCase testCase;

        private TestMethodWriterWithThreadsParameterResolver(TestMethodWriterWithThreadsCase testCase) {
            this.testCase = testCase;
        }

        @Override
        public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            Class<?> type = parameterContext.getParameter().getType();
            return type == boolean.class || type == Boolean.class;
        }

        @Override
        public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            return testCase.doubleBuffer;
        }
    }

    @BeforeEach
    public void check64bit() {
        assumeTrue(Jvm.is64bit());
    }

    @Override
    @BeforeEach
    public void threadDump() {
        super.threadDump();
    }

    @TestTemplate
    @Timeout(value = 30_000, unit = TimeUnit.MILLISECONDS)
    public void test() throws FileNotFoundException {

        File tmpDir = getTmpDir();
        try (final ChronicleQueue q = builder(tmpDir, WireType.BINARY).rollCycle(HOURLY).doubleBuffer(doubleBuffer).build()) {

            methodWriter = q.methodWriter(I.class);

            IntStream.range(0, 1000)
                    .parallel()
                    .forEach(i -> {
                        try (final ExcerptTailer tailer = q.createTailer()) {
                            creates();
                            amends();
                            final MethodReader methodReader = tailer.methodReader(newReader());
                            for (int j = 0; j < 2 && !fail.get(); )
                                if (methodReader.readOne())
                                    j++;
                        } finally {
                            // close appender acquired by creates above
                            Closeable.closeQuietly(acquireThreadLocalAppender(q));
                        }
                        if (fail.get())
                            fail("methodReader validation failed");
                    });
            assertFalse(fail.get(), "methodWriter threads: no validation failures");

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
