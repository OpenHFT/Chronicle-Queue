/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.AbstractReferenceCounted;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.core.onoes.LogLevel;
import net.openhft.chronicle.core.threads.CleaningThread;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.queue.util.HugetlbfsTestUtil;
import net.openhft.chronicle.testframework.exception.ExceptionTracker;
import net.openhft.chronicle.wire.MessageHistory;
import net.openhft.chronicle.wire.VanillaMethodWriterBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.*;
import org.junit.runner.Description;

import java.io.File;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static net.openhft.chronicle.core.onoes.LogLevel.DEBUG;
import static net.openhft.chronicle.core.onoes.LogLevel.PERF;

public class QueueTestCommon {
    private static final Set<LogLevel> IGNORED_LOG_LEVELS = EnumSet.of(DEBUG, PERF);
    private static final boolean TRACE_TEST_EXECUTION = Jvm.getBoolean("queue.traceTestExecution");
    private static final String METHOD_WRITER_FALLBACK =
            "Failed to compile generated method writer - falling back to proxy method writer";
    private final List<File> tmpDirs = new ArrayList<>();

    private ThreadDump threadDump;
    protected boolean finishedNormally;
    protected ExceptionTracker<ExceptionKey> exceptionTracker;
    private Map<ExceptionKey, Integer> recordedExceptions;
    private String diagnosticTestName = getClass().getName();

    static {
        System.setProperty("queue.check.index", "true");
    }

    // JUNIT Rules
    // catch-all timeout for when it has not been specified
    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    @Rule
    public final TestName testName = new TestName();

    @Rule
    public final ErrorCollector errorCollector = new ErrorCollector();

    @NotNull
    @Rule
    public TestRule watcher = new TestWatcher() {
        @Override
        protected void starting(@NotNull Description description) {
            // A subclass can hide the inherited TestName rule; the runner still supplies its identity here.
            diagnosticTestName = description.getClassName() + "." + description.getMethodName();
            if (TRACE_TEST_EXECUTION) {
                Jvm.debug().on(getClass(), "Starting test: "
                        + description.getClassName() + "."
                        + description.getMethodName()
                );
                traceResourceBoundary("start");
            }
        }

        @Override
        protected void finished(@NotNull Description description) {
            if (TRACE_TEST_EXECUTION)
                traceResourceBoundary("finish");
        }
    };

    //! Opt-in boundaries correlate per-process native/mapped samples with the active test.
    //! Used/committed heap are observations, not per-test ownership or a resource limit.
    //! Write directly so exception recording does not hide this diagnostic or turn it into a failure.
    private void traceResourceBoundary(String phase) {
        Runtime runtime = Runtime.getRuntime();
        System.out.println("QueueTestExecution phase=" + phase + " timeMs=" + System.currentTimeMillis()
                + " pid=" + OS.getProcessId() + " test=" + diagnosticTestName
                + " heapUsed=" + (runtime.totalMemory() - runtime.freeMemory())
                + " heapCommitted=" + runtime.totalMemory() + " heapMax=" + runtime.maxMemory()
                + " target=" + OS.getTarget());
    }

    private static AtomicLong counter = new AtomicLong();
    private Set<String> targetAllowList;
    private long freeSpace;

    @NotNull
    protected File getTmpDir() {
        final String methodName = testName.getMethodName();
        String name = methodName == null ? "unknown" : methodName;
        final File tmpDir = DirectoryUtils.tempDir(name + "-" + counter.incrementAndGet());
        tmpDirs.add(tmpDir);
        return tmpDir;
    }

    /**
     * @see #deleteTargetDirTestArtifacts()
     */
    @Before
    public void recordTargetDirContents() {
        String target = OS.getTarget();
        File[] files = new File(target).listFiles();
        if (files == null) {
            targetAllowList = Collections.emptySet();
        } else {
            targetAllowList = Stream.of(files)
                    .map(File::getName)
                    .collect(Collectors.toSet());
        }
    }

    @Before
    public void recordDiskSpace() {
        freeSpace = diskFreeSpace();
    }

    @After
    public void checkSpaceUsed() {
        long spaceLeft = diskFreeSpace();
        if (freeSpace - spaceLeft > 2L << 30) {
            //! Free space belongs to the filesystem shared by all test forks and other
            //! processes. A decrease during this test cannot identify which owner wrote
            //! the data, so keep the 2 GiB observation as context rather than a failure.
            //! Agent/build capacity checks must enforce headroom; fixture limits require
            //! explicitly owned paths. Use stdout because an error/warning handler can
            //! turn this diagnostic back into a test failure through ExceptionTracker.
            System.out.println("Shared filesystem free space decreased by "
                    + ((freeSpace - spaceLeft) >> 20) / 1024.0 + " GiB during "
                    + getClass().getName() + "." + testName.getMethodName()
                    + " (target " + OS.getTarget() + "); this is not per-test disk usage.");
        }
    }

    long diskFreeSpace() {
        return new File(OS.getTarget()).getFreeSpace();
    }

    @Before
    public void assumeFinishedNormally() {
        finishedNormally = true;
    }

    @Before
    public void clearMessageHistory() {
        MessageHistory.get().reset();
    }

    @Before
    public void enableReferenceTracing() {
        AbstractReferenceCounted.enableReferenceTracing();
    }

    public void assertReferencesReleased() {
        AbstractReferenceCounted.assertReferencesReleased();
    }

    // add @Before to sub class where a thread might be added
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    private void checkThreadDump() {
        if (threadDump != null)
            threadDump.assertNoNewThreads();
    }

    @Before
    public void recordExceptions() {
        recordedExceptions = Jvm.recordExceptions(false);
        exceptionTracker = ExceptionTracker.create(
                ExceptionKey::message,
                ExceptionKey::throwable,
                Jvm::resetExceptionHandlers,
                recordedExceptions,
                key -> IGNORED_LOG_LEVELS.contains(key.level()),
                key -> key.level() + " " + key.clazz().getSimpleName() + " " + key.message()
        );
        if (OS.isWindows())
            ignoreException("Read-only mode is not supported on Windows® platforms, defaulting to read/write");
        for (String msg : Arrays.asList(
                "Shrinking ",
                "Allocation of ",
                " ms to add mapping for ",
                "jar to the classpath",
                " ms to pollDiskSpace for ",
                " us to linearScan by position from ",
                "File released ",
                "Overriding roll length from existing metadata",
                " was 3600000",
                " overriding to 86400000   ",
                "does not free direct memory")) {
            ignoreException(msg);
        }
    }

    protected void ignoreException(String message) {
        exceptionTracker.ignoreException(message);
    }

    protected void expectException(String message) {
        exceptionTracker.expectException(message);
    }

    protected void ignoreException(Predicate<ExceptionKey> predicate, String description) {
        exceptionTracker.ignoreException(predicate, description);
    }

    public void expectException(Predicate<ExceptionKey> predicate, String description) {
        exceptionTracker.expectException(predicate, description);
    }

    private void checkExceptions() {
        exceptionTracker.checkExceptions();
    }

    /**
     * When running tests on hugetlbfs queue files all take up pages in the CI environment. Historically not all tests
     * neatly clean up their test data after they exit and this meant that hugetlbfs CI tests would run out of huge
     * pages to allocate. To work around this when running in the context of hugetlbfs the below method will ensure
     * that any files created in the OS.getTarget() directory are cleaned up in between tests to prevent the host from
     * running out of huge pages during the build.
     *
     * @see #recordTargetDirContents() which tracks the original contents of target and avoids deleting unrelated files
     */
    @After
    public void deleteTargetDirTestArtifacts() {
        if (HugetlbfsTestUtil.isHugetlbfsAvailable()) {
            String target = OS.getTarget();
            File[] files = new File(target).listFiles();
            if (files == null) {
                return;
            }
            Set<String> currentFilesInTarget = Stream.of(files)
                    .map(File::getName)
                    .collect(Collectors.toSet());

            currentFilesInTarget.stream()
                    .filter(fileName -> !targetAllowList.contains(fileName))
                    .forEach(fileName -> {
                        try {
                            IOTools.deleteDirWithFiles(Paths.get(target, fileName).toFile());
                        } catch (Exception e) {
                            Jvm.error().on(this.getClass(), "Could not delete file - " + fileName, e);
                        }
                    });
        }
    }

    @After
    public void afterChecks() {
        // Report before cleanup can fail or exception checking can consume an expected warning.
        reportMethodWriterFallback();
        preAfter();
        SystemTimeProvider.CLOCK = SystemTimeProvider.INSTANCE;
        CleaningThread.performCleanup(Thread.currentThread());

        // find any discarded resources.
        AbstractCloseable.waitForCloseablesToClose(100);

        if (finishedNormally) {
            assertReferencesReleased();
            checkThreadDump();
            checkExceptions();
        }

        tearDown();
    }

    private void reportMethodWriterFallback() {
        try {
            if (recordedExceptions == null)
                return;
            Map.Entry<ExceptionKey, Integer> first = null;
            synchronized (recordedExceptions) {
                for (Map.Entry<ExceptionKey, Integer> entry : recordedExceptions.entrySet()) {
                    ExceptionKey key = entry.getKey();
                    if (key.level() == LogLevel.WARN
                            && VanillaMethodWriterBuilder.class.isAssignableFrom(key.clazz())
                            && key.message() != null && key.message().startsWith(METHOD_WRITER_FALLBACK)) {
                        first = new AbstractMap.SimpleImmutableEntry<>(entry);
                        break;
                    }
                }
            }
            if (first == null)
                return;

            // Bound the extra output to the first distinct warning and its recorded occurrence count.
            PrintStream out = methodWriterDiagnosticStream();
            out.println("Method writer fallback diagnostic (count=" + first.getValue()
                    + ", test=" + diagnosticTestName
                    + ", java.version=" + System.getProperty("java.version")
                    + ", wire.generator.v2=" + System.getProperty("wire.generator.v2")
                    + ", disableProxyCodegen=" + System.getProperty("disableProxyCodegen")
                    + "): " + first.getKey().message());
            if (first.getKey().throwable() != null)
                first.getKey().throwable().printStackTrace(out);
        } catch (Throwable ignored) {
            // Best-effort evidence must neither create a failure nor replace the original one.
        }
    }

    PrintStream methodWriterDiagnosticStream() {
        // Jvm.warn() would add another event to the collection being checked by this fixture.
        return System.err;
    }

    protected void preAfter() {
    }

    protected void tearDown() {
        // File deletion follows deferred unmapping. Report every remaining owned path
        // as a failure: exception tracking has already finished by this point.
        net.openhft.chronicle.core.io.BackgroundResourceReleaser.releasePendingResources();
        List<File> remaining = new ArrayList<>();
        tmpDirs.forEach(file -> {
            if (file.exists() && !IOTools.deleteDirWithFiles(file)) {
                remaining.add(file);
            }
        });
        if (!remaining.isEmpty())
            throw new AssertionError("Could not delete owned test directories: " + remaining);
    }
}
