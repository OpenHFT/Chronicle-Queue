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
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static net.openhft.chronicle.core.onoes.LogLevel.DEBUG;
import static net.openhft.chronicle.core.onoes.LogLevel.PERF;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(60)
public class QueueTestCommon {
    private static final Set<LogLevel> IGNORED_LOG_LEVELS = EnumSet.of(DEBUG, PERF);
    private static final boolean TRACE_TEST_EXECUTION = Jvm.getBoolean("queue.traceTestExecution");
    private final List<File> tmpDirs = new ArrayList<>();

    private ThreadDump threadDump;
    protected boolean finishedNormally;
    protected ExceptionTracker<ExceptionKey> exceptionTracker;

    static {
        System.setProperty("queue.check.index", "true");
    }

    protected String testMethodName;

    @BeforeEach
    void beforeEachQueueTestCommon(TestInfo testInfo) {
        initTestInfo(testInfo);
        recordTargetDirContents();
        recordDiskSpace();
        assumeFinishedNormally();
        clearMessageHistory();
        enableReferenceTracing();
        recordExceptions();
    }

    public void initTestInfo(TestInfo testInfo) {
        testMethodName = testInfo.getTestMethod().map(java.lang.reflect.Method::getName).orElse("unknown");
        if (TRACE_TEST_EXECUTION) {
            Jvm.debug().on(getClass(), "Starting test: "
                    + testInfo.getTestClass().map(Class::getName).orElse("unknown") + "."
                    + testMethodName
            );
        }
    }

    private static AtomicLong counter = new AtomicLong();
    private Set<String> targetAllowList;
    private long freeSpace;

    @NotNull
    protected File getTmpDir() {
        String name = testMethodName == null ? "unknown" : testMethodName;
        final File tmpDir = DirectoryUtils.tempDir(name + "-" + counter.incrementAndGet());
        tmpDirs.add(tmpDir);
        return tmpDir;
    }

    /**
     * @see #deleteTargetDirTestArtifacts()
     */
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

    public void recordDiskSpace() {
        freeSpace = new File(OS.getTarget()).getFreeSpace();
    }

    @AfterEach
    void afterEachQueueTestCommon() {
        checkSpaceUsed();
        deleteTargetDirTestArtifacts();
        afterChecks();
    }

    public void checkSpaceUsed() {
        long spaceLeft = new File(OS.getTarget()).getFreeSpace();
        if (freeSpace - spaceLeft > 2L << 30) {
            fail("Used more than 1 GB of disk space in " + OS.getTarget() + " during the test, was " + ((freeSpace - spaceLeft) >> 20) / 1024.0 + " GiB");
        }
    }

    public void assumeFinishedNormally() {
        finishedNormally = true;
    }

    public void clearMessageHistory() {
        MessageHistory.get().reset();
    }

    public void enableReferenceTracing() {
        AbstractReferenceCounted.enableReferenceTracing();
    }

    public void assertReferencesReleased() {
        AbstractReferenceCounted.assertReferencesReleased();
    }

    // add @BeforeEach to sub class where a thread might be added
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    private void checkThreadDump() {
        if (threadDump != null)
            threadDump.assertNoNewThreads();
    }

    public void recordExceptions() {
        Map<ExceptionKey, Integer> recordedExceptions = Jvm.recordExceptions(false);
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
                " overriding to 86400000   ")) {
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

    public void afterChecks() {
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

    protected void preAfter() {
    }

    protected void tearDown() {
        // should be able to remove tmp dirs
        tmpDirs.forEach(file -> {
            if (file.exists() && !IOTools.deleteDirWithFiles(file)) {
                Jvm.error().on(getClass(), "Could not delete tmp dir " + file);
            }
        });
    }
}
