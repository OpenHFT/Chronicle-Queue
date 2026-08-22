/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.AbstractGeneratedMethodReader;
import net.openhft.chronicle.wire.VanillaMethodReader;
import net.openhft.chronicle.wire.VanillaMethodReaderBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static net.openhft.chronicle.queue.TailerContextRecovery.Status.CYCLE_NOT_AVAILABLE;
import static net.openhft.chronicle.queue.TailerContextRecovery.Status.NO_RESUME_POSITION;
import static net.openhft.chronicle.queue.TailerContextRecovery.Status.REPLAYED_TO_RESUME_INDEX;
import static net.openhft.chronicle.queue.TailerContextRecovery.Status.RESUME_INDEX_AT_CYCLE_START;
import static net.openhft.chronicle.queue.TailerContextRecovery.Status.REPLAY_INCOMPLETE;
import static net.openhft.chronicle.queue.TailerDirection.BACKWARD;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static net.openhft.chronicle.wire.VanillaMethodReaderBuilder.DISABLE_READER_PROXY_CODEGEN;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

public class TailerContextRecoveryTest extends QueueTestCommon {

    private static final long INITIAL_TIME_NANOS = 1_000_000_000L;

    @Test
    public void requiresForwardTailerAndReplayHandlers() {
        assertThrows(NullPointerException.class,
                () -> TailerContextRecovery.replayCurrentCycleContext(
                        (ExcerptTailer) null, new RecordingReplayHandler()));

        File path = getTmpDir();
        try (ChronicleQueue queue = queueBuilder(path, new SetTimeProvider(INITIAL_TIME_NANOS)).build();
             ExcerptTailer tailer = queue.createTailer()) {
            assertThrows(IllegalArgumentException.class,
                    () -> TailerContextRecovery.replayCurrentCycleContext(tailer));
            assertThrows(NullPointerException.class,
                    () -> TailerContextRecovery.replayCurrentCycleContext(tailer, (Object) null));

            tailer.direction(BACKWARD);
            assertThrows(IllegalArgumentException.class,
                    () -> TailerContextRecovery.replayCurrentCycleContext(tailer, new RecordingReplayHandler()));
        }

        assertThrows(NullPointerException.class,
                () -> TailerContextRecovery.replayCurrentCycleContext(
                        (ChronicleQueue) null, 1, new RecordingReplayHandler()));
    }

    @Test
    public void restartedNamedTailerRebuildsCurrentCycleContextBeforeContinuing() {
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(INITIAL_TIME_NANOS);

        long resumeIndex;
        try (ChronicleQueue queue = marketQueueBuilder(path, timeProvider).build()) {
            try (ExcerptAppender appender = queue.createAppender()) {
                MarketEvents writer = appender.methodWriter(MarketEvents.class);
                writer.trade(17, 10);
                writer.trade(17, 20);
            }

            MarketProjection firstRunProjection = new MarketProjection();
            MarketEventHandler firstRunHandler = new MarketEventHandler(firstRunProjection);
            try (ExcerptTailer namedTailer = queue.createTailer("risk-engine")) {
                MethodReader reader = namedTailer.methodReader(firstRunHandler);
                assertTrue(reader.readOne());
                assertTrue(reader.readOne());
                resumeIndex = namedTailer.index();
            }
            assertEquals(Collections.singletonList("EUR/USD:10"), firstRunHandler.trades);
        }

        MarketProjection restartedProjection = new MarketProjection();
        try (ChronicleQueue queue = marketQueueBuilder(path, timeProvider).build();
             ExcerptTailer restartedTailer = queue.createTailer("risk-engine")) {
            assertEquals(resumeIndex, restartedTailer.index());

            TailerContextRecovery.ReplayResult result =
                    TailerContextRecovery.replayCurrentCycleContext(
                            restartedTailer, restartedProjection, new NoOpMarketDataHandler());

            assertEquals(REPLAYED_TO_RESUME_INDEX, result.status());
            assertTrue(result.complete());
            assertEquals(2, result.documentsScanned());
            assertEquals("EUR/USD", restartedProjection.symbolName(17));
            assertEquals("context replay must not advance the resume tailer",
                    resumeIndex, restartedTailer.index());

            MarketEventHandler restartedHandler = new MarketEventHandler(restartedProjection);
            MethodReader reader = restartedTailer.methodReader(restartedHandler);
            assertTrue(reader.readOne());
            assertEquals(Collections.singletonList("EUR/USD:20"), restartedHandler.trades);
            assertFalse(reader.readOne());
        }
    }

    @Test
    public void freshTailerHasNoResumePosition() {
        File path = getTmpDir();

        try (ChronicleQueue queue = queueBuilder(path, new SetTimeProvider(INITIAL_TIME_NANOS)).build();
             ExcerptTailer tailer = queue.createTailer("fresh")) {
            TailerContextRecovery.ReplayResult result =
                    TailerContextRecovery.replayCurrentCycleContext(tailer, new RecordingReplayHandler());

            assertEquals(NO_RESUME_POSITION, result.status());
            assertTrue(result.complete());
            assertEquals(0, result.documentsScanned());
            assertEquals(0, tailer.index());
        }
    }

    @Test
    public void resumeIndexAtCycleStartNeedsNoReplayAndSupportsUnnamedTailer() {
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(INITIAL_TIME_NANOS);

        try (ChronicleQueue queue = queueBuilder(path, timeProvider).build()) {
            writeReplayEvents(queue, "first", "second");
            try (ExcerptTailer tailer = queue.createTailer()) {
                long resumeIndex = tailer.index();
                assertEquals(0, queue.rollCycle().toSequenceNumber(resumeIndex));

                TailerContextRecovery.ReplayResult result =
                        TailerContextRecovery.replayCurrentCycleContext(tailer, new RecordingReplayHandler());

                assertEquals(RESUME_INDEX_AT_CYCLE_START, result.status());
                assertTrue(result.complete());
                assertEquals(0, result.documentsScanned());
                assertEquals(resumeIndex, tailer.index());
            }
        }
    }

    @Test
    public void unavailableResumeCycleIsReported() throws IOException {
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(INITIAL_TIME_NANOS);
        long resumeIndex;

        try (ChronicleQueue queue = queueBuilder(path, timeProvider).build()) {
            writeReplayEvents(queue, "one", "two", "three");
            resumeIndex = persistAfterReading(queue, "missing-cycle", 2);
        }

        File[] cycleFiles = cycleFiles(path);
        assertEquals(1, cycleFiles.length);
        Files.delete(cycleFiles[0].toPath());

        try (ChronicleQueue queue = queueBuilder(path, timeProvider).build();
             ExcerptTailer tailer = queue.createTailer("missing-cycle")) {
            assertEquals(resumeIndex, tailer.index());

            TailerContextRecovery.ReplayResult result =
                    TailerContextRecovery.replayCurrentCycleContext(tailer, new RecordingReplayHandler());

            assertEquals(CYCLE_NOT_AVAILABLE, result.status());
            assertFalse(result.complete());
            assertEquals(0, result.documentsScanned());
            assertEquals(resumeIndex, tailer.index());
        }
    }

    @Test
    public void readOnlyRestartCanReplayPersistedPosition() {
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(INITIAL_TIME_NANOS);
        long resumeIndex;

        try (ChronicleQueue queue = queueBuilder(path, timeProvider).build()) {
            writeReplayEvents(queue, "one", "two", "three");
            resumeIndex = persistAfterReading(queue, "read-only", 2);
        }

        RecordingReplayHandler replayHandler = new RecordingReplayHandler();
        try (ChronicleQueue queue = queueBuilder(path, timeProvider)
                .readOnly(true)
                .build()) {
            TailerContextRecovery.ReplayResult result =
                    TailerContextRecovery.replayCurrentCycleContext(queue, resumeIndex, replayHandler);

            assertEquals(REPLAYED_TO_RESUME_INDEX, result.status());
            assertTrue(result.complete());
            assertEquals(Arrays.asList("one", "two"), replayHandler.values);
        }
    }

    @Test
    public void missingReplayMethodFailsGeneratedReader() {
        missingReplayMethodFails(false);
    }

    @Test
    public void missingReplayMethodFailsFallbackReader() {
        missingReplayMethodFails(true);
    }

    @Test
    public void shortenedCycleStopsWithoutDispatchingLaterCycle() throws IOException {
        shortenedCycleStopsBeforeLaterCycle();
    }

    @Test
    public void generatedReaderChecksActualDocumentBeforeDispatch() {
        forcedReadingDocumentCrossingDoesNotDispatch(false);
    }

    @Test
    public void fallbackReaderChecksActualDocumentBeforeDispatch() {
        forcedReadingDocumentCrossingDoesNotDispatch(true);
    }

    private void shortenedCycleStopsBeforeLaterCycle() throws IOException {
        assumeFalse(OS.isWindows());
        File root = getTmpDir();
        File primaryPath = new File(root, "primary");
        File shortenedPath = new File(root, "shortened");
        SetTimeProvider primaryTime = new SetTimeProvider(INITIAL_TIME_NANOS);
        long resumeIndex;

        try (ChronicleQueue queue = queueBuilder(primaryPath, primaryTime).build()) {
            writeReplayEvents(queue, "long-0", "long-1", "long-2", "long-3", "long-4", "long-5");
            resumeIndex = persistAfterReading(queue, "shortened-cycle", 5);

            primaryTime.advanceMillis(1_000);
            writeReplayEvents(queue, "deleted-following-cycle");
            primaryTime.advanceMillis(1_000);
            writeReplayEvents(queue, "later-cycle");
        }

        try (ChronicleQueue shortenedQueue = queueBuilder(
                shortenedPath, new SetTimeProvider(INITIAL_TIME_NANOS)).build()) {
            writeReplayEvents(shortenedQueue, "short-0", "short-1");
        }

        File[] shortenedCycleFiles = cycleFiles(shortenedPath);
        assertEquals(1, shortenedCycleFiles.length);
        File replacement = new File(primaryPath, shortenedCycleFiles[0].getName());
        assertTrue("expected the original resume cycle", replacement.isFile());
        Files.copy(shortenedCycleFiles[0].toPath(), replacement.toPath(), REPLACE_EXISTING);

        File[] primaryCycleFiles = cycleFiles(primaryPath);
        assertEquals(3, primaryCycleFiles.length);
        DeletingRecordingReplayHandler replayHandler =
                new DeletingRecordingReplayHandler(primaryCycleFiles[0], primaryCycleFiles[1]);
        try (ChronicleQueue queue = queueBuilder(primaryPath, primaryTime).build();
             ExcerptTailer tailer = queue.createTailer("shortened-cycle")) {
            assertEquals(resumeIndex, tailer.index());
            int resumeCycle = queue.rollCycle().toCycle(resumeIndex);

            TailerContextRecovery.ReplayResult result =
                    TailerContextRecovery.replayCurrentCycleContext(tailer, replayHandler);

            assertEquals(REPLAY_INCOMPLETE, result.status());
            assertFalse(result.complete());
            assertEquals(Arrays.asList("short-0", "short-1"), replayHandler.values);
            assertEquals("the resume tailer must remain unchanged", resumeIndex, tailer.index());
            assertEquals(resumeCycle, queue.rollCycle().toCycle(result.lastScannedIndex()));
        }
    }

    private void forcedReadingDocumentCrossingDoesNotDispatch(boolean disableCodegen) {
        // MethodReader evaluates its predicate before readingDocument(). These delegating views
        // deterministically make readingDocument() select a later-cycle record after that predicate,
        // so the test exercises the dispatch-time guard rather than only the pre-read optimisation.
        String previousCodegenSetting = System.getProperty(DISABLE_READER_PROXY_CODEGEN);
        System.setProperty(DISABLE_READER_PROXY_CODEGEN, Boolean.toString(disableCodegen));
        try {
            File path = getTmpDir();
            SetTimeProvider timeProvider = new SetTimeProvider(INITIAL_TIME_NANOS);
            try (ChronicleQueue queue = queueBuilder(path, timeProvider).build();
                 ExcerptAppender appender = queue.createAppender()) {
                ReplayEvents writer = appender.methodWriter(ReplayEvents.class);
                for (int i = 0; i < 6; i++)
                    writer.context("resume-cycle-" + i);

                long lastResumeCycleIndex = queue.lastIndex();
                int resumeCycle = queue.rollCycle().toCycle(lastResumeCycleIndex);
                long resumeIndex = queue.rollCycle().toIndex(resumeCycle, 5);

                timeProvider.advanceMillis(1_000);
                writer.context("later-cycle");
                long laterIndex = queue.lastIndex();

                try (ExcerptTailer realResumeTailer = queue.createTailer()) {
                    assertTrue(realResumeTailer.moveToIndex(resumeIndex));
                    ExcerptTailer realReplayTailer = queue.createTailer();
                    AtomicReference<ExcerptTailer> replayTailerReference = new AtomicReference<>();
                    AtomicReference<MethodReader> methodReaderReference = new AtomicReference<>();
                    ChronicleQueue queueView = queueReturningReplayTailer(queue, replayTailerReference);
                    ExcerptTailer crossingReplayTailer = crossingReplayTailer(
                            realReplayTailer, queueView, resumeCycle, laterIndex, methodReaderReference);
                    replayTailerReference.set(crossingReplayTailer);
                    ExcerptTailer resumeTailerView = tailerWithQueue(realResumeTailer, queueView);
                    RecordingReplayHandler replayHandler = new RecordingReplayHandler();

                    TailerContextRecovery.ReplayResult result =
                            TailerContextRecovery.replayCurrentCycleContext(resumeTailerView, replayHandler);

                    assertEquals(REPLAY_INCOMPLETE, result.status());
                    assertFalse(result.complete());
                    assertTrue("the later-cycle handler must not be invoked", replayHandler.values.isEmpty());
                    assertEquals(laterIndex, result.lastScannedIndex());
                    assertEquals(resumeIndex, realResumeTailer.index());
                    if (disableCodegen)
                        assertTrue(methodReaderReference.get() instanceof VanillaMethodReader);
                    else
                        assertTrue(methodReaderReference.get() instanceof AbstractGeneratedMethodReader);
                }
            }
        } finally {
            restoreProperty(DISABLE_READER_PROXY_CODEGEN, previousCodegenSetting);
        }
    }

    private static ChronicleQueue queueReturningReplayTailer(
            ChronicleQueue delegate, AtomicReference<ExcerptTailer> replayTailerReference) {
        return (ChronicleQueue) Proxy.newProxyInstance(
                ChronicleQueue.class.getClassLoader(),
                new Class<?>[]{ChronicleQueue.class},
                (proxy, method, arguments) -> {
                    if ("createTailer".equals(method.getName()) &&
                            (arguments == null || arguments.length == 0))
                        return replayTailerReference.get();
                    return invokeDelegate(method, delegate, arguments);
                });
    }

    private static ExcerptTailer tailerWithQueue(ExcerptTailer delegate, ChronicleQueue queueView) {
        return (ExcerptTailer) Proxy.newProxyInstance(
                ExcerptTailer.class.getClassLoader(),
                new Class<?>[]{ExcerptTailer.class},
                (proxy, method, arguments) -> "queue".equals(method.getName())
                        ? queueView
                        : invokeDelegate(method, delegate, arguments));
    }

    private static ExcerptTailer crossingReplayTailer(
            ExcerptTailer delegate,
            ChronicleQueue queueView,
            int resumeCycle,
            long laterIndex,
            AtomicReference<MethodReader> methodReaderReference) {
        AtomicReference<ExcerptTailer> proxyReference = new AtomicReference<>();
        boolean[] beforeRead = {true};
        ExcerptTailer proxy = (ExcerptTailer) Proxy.newProxyInstance(
                ExcerptTailer.class.getClassLoader(),
                new Class<?>[]{ExcerptTailer.class},
                (ignored, method, arguments) -> {
                    switch (method.getName()) {
                        case "queue":
                            return queueView;
                        case "moveToCycle":
                            beforeRead[0] = true;
                            return ((Integer) arguments[0]) == resumeCycle;
                        case "index":
                            return beforeRead[0]
                                    ? queueView.rollCycle().toIndex(resumeCycle, 0)
                                    : delegate.index();
                        case "methodReaderBuilder":
                            return new CapturingMethodReaderBuilder(
                                    proxyReference.get(), methodReaderReference);
                        case "readingDocument":
                            beforeRead[0] = false;
                            assertTrue(delegate.moveToIndex(laterIndex));
                            return arguments == null || arguments.length == 0
                                    ? delegate.readingDocument()
                                    : delegate.readingDocument((Boolean) arguments[0]);
                        default:
                            return invokeDelegate(method, delegate, arguments);
                    }
                });
        proxyReference.set(proxy);
        return proxy;
    }

    private void missingReplayMethodFails(boolean disableCodegen) {
        String previousCodegenSetting = System.getProperty(DISABLE_READER_PROXY_CODEGEN);
        System.setProperty(DISABLE_READER_PROXY_CODEGEN, Boolean.toString(disableCodegen));
        try {
            File path = getTmpDir();
            SetTimeProvider timeProvider = new SetTimeProvider(INITIAL_TIME_NANOS);
            try (ChronicleQueue queue = marketQueueBuilder(path, timeProvider).build()) {
                try (ExcerptAppender appender = queue.createAppender()) {
                    MarketEvents writer = appender.methodWriter(MarketEvents.class);
                    writer.trade(17, 10);
                }
                long resumeIndex = persistAfterReading(queue, "missing-handler", 1);

                try (ExcerptTailer tailer = queue.createTailer()) {
                    assertTrue(tailer.moveToIndex(resumeIndex));
                    IllegalStateException exception = assertThrows(IllegalStateException.class,
                            () -> TailerContextRecovery.replayCurrentCycleContext(
                                    tailer, new NoOpMarketDataHandler()));
                    assertTrue(exception.getMessage(), exception.getMessage().contains("instrument"));
                    assertEquals(resumeIndex, tailer.index());
                }
            }
        } finally {
            restoreProperty(DISABLE_READER_PROXY_CODEGEN, previousCodegenSetting);
        }
    }

    private static final class CapturingMethodReaderBuilder extends VanillaMethodReaderBuilder {
        private final AtomicReference<MethodReader> methodReaderReference;

        private CapturingMethodReaderBuilder(
                ExcerptTailer tailer, AtomicReference<MethodReader> methodReaderReference) {
            super(tailer);
            this.methodReaderReference = methodReaderReference;
        }

        @Override
        public MethodReader build(Object... implementations) {
            MethodReader methodReader = super.build(implementations);
            methodReaderReference.set(methodReader);
            return methodReader;
        }
    }

    private static Object invokeDelegate(Method method, Object delegate, Object[] arguments) throws Throwable {
        try {
            return method.invoke(delegate, arguments);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static SingleChronicleQueueBuilder queueBuilder(File path, SetTimeProvider timeProvider) {
        return SingleChronicleQueueBuilder.binary(path)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(timeProvider);
    }

    private static SingleChronicleQueueBuilder marketQueueBuilder(File path, SetTimeProvider timeProvider) {
        return queueBuilder(path, timeProvider)
                .contextListener(MarketContext.class, context -> context.instrument(17, "EUR/USD"));
    }

    private static void writeReplayEvents(ChronicleQueue queue, String... values) {
        try (ExcerptAppender appender = queue.createAppender()) {
            ReplayEvents writer = appender.methodWriter(ReplayEvents.class);
            for (String value : values)
                writer.context(value);
        }
    }

    private static long persistAfterReading(ChronicleQueue queue, String tailerName, int documents) {
        RecordingReplayHandler handler = new RecordingReplayHandler();
        try (ExcerptTailer tailer = queue.createTailer(tailerName)) {
            MethodReader reader = tailer.methodReader(handler);
            for (int i = 0; i < documents; i++)
                assertTrue(reader.readOne());
            return tailer.index();
        }
    }

    private static File[] cycleFiles(File path) {
        File[] files = path.listFiles((directory, name) -> name.endsWith(".cq4"));
        assertNotNull(files);
        Arrays.sort(files, Comparator.comparing(File::getName));
        return files;
    }

    private static void restoreProperty(String property, String previousValue) {
        if (previousValue == null)
            System.clearProperty(property);
        else
            System.setProperty(property, previousValue);
    }

    public interface ReplayEvents {
        void context(String value);
    }

    public static final class RecordingReplayHandler implements ReplayEvents {
        private final List<String> values = new ArrayList<>();

        @Override
        public void context(String value) {
            values.add(value);
        }
    }

    public static final class DeletingRecordingReplayHandler implements ReplayEvents {
        private final List<String> values = new ArrayList<>();
        private final File currentCycle;
        private final File followingCycle;
        private boolean deleted;

        private DeletingRecordingReplayHandler(File currentCycle, File followingCycle) {
            this.currentCycle = currentCycle;
            this.followingCycle = followingCycle;
        }

        @Override
        public void context(String value) {
            values.add(value);
            if (deleted)
                return;
            deleted = true;
            try {
                Files.delete(currentCycle.toPath());
                Files.delete(followingCycle.toPath());
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
    }

    public interface MarketContext {
        void instrument(int symbolId, String name);
    }

    public interface MarketData {
        void trade(int symbolId, long quantity);
    }

    public interface MarketEvents extends MarketContext, MarketData {
    }

    public static final class MarketProjection implements MarketContext {
        private final Map<Integer, String> symbolNames = new HashMap<>();

        @Override
        public void instrument(int symbolId, String name) {
            symbolNames.put(symbolId, name);
        }

        private String symbolName(int symbolId) {
            return symbolNames.get(symbolId);
        }
    }

    public static final class NoOpMarketDataHandler implements MarketData {
        @Override
        public void trade(int symbolId, long quantity) {
            // Replay must consume ordinary data methods without repeating their business effects.
        }
    }

    public static final class MarketEventHandler implements MarketEvents {
        private final MarketProjection projection;
        private final List<String> trades = new ArrayList<>();

        private MarketEventHandler(MarketProjection projection) {
            this.projection = projection;
        }

        @Override
        public void instrument(int symbolId, String name) {
            projection.instrument(symbolId, name);
        }

        @Override
        public void trade(int symbolId, long quantity) {
            String symbolName = projection.symbolName(symbolId);
            if (symbolName == null)
                throw new IllegalStateException("Missing instrument context for " + symbolId);
            trades.add(symbolName + ":" + quantity);
        }
    }
}
