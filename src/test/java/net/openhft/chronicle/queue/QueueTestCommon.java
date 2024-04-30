/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.internal.JvmExceptionTracker;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.AbstractReferenceCounted;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.core.threads.CleaningThread;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.queue.util.HugetlbfsTestUtil;
import net.openhft.chronicle.testframework.exception.ExceptionTracker;
import net.openhft.chronicle.wire.MessageHistory;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.*;
import org.junit.runner.Description;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueueTestCommon {
    private static final boolean TRACE_TEST_EXECUTION = Jvm.getBoolean("queue.traceTestExecution");
    private final List<File> tmpDirs = new ArrayList<>();

    protected ThreadDump threadDump;
    protected boolean finishedNormally;
    protected ExceptionTracker<ExceptionKey> exceptionTracker;

    static {
        System.setProperty("queue.check.index", "true");
    }

    // *************************************************************************
    // JUNIT Rules
    // *************************************************************************

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
            if (TRACE_TEST_EXECUTION) {
                Jvm.debug().on(getClass(), "Starting test: "
                        + description.getClassName() + "."
                        + description.getMethodName()
                );
            }
        }
    };

    // *************************************************************************
    //
    // *************************************************************************
    static AtomicLong counter = new AtomicLong();
    private Set<String> targetAllowList;

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

    public void checkThreadDump() {
        if (threadDump != null)
            threadDump.assertNoNewThreads();
    }

    @Before
    public void recordExceptions() {
        exceptionTracker = JvmExceptionTracker.create(false);
        if (OS.isWindows())
            ignoreException("Read-only mode is not supported on Windows® platforms, defaulting to read/write");
        for (String msg : "Shrinking ,Allocation of , ms to add mapping for ,jar to the classpath, ms to pollDiskSpace for , us to linearScan by position from ,File released ,Overriding roll length from existing metadata, was 3600000, overriding to 86400000   ".split(",")) {
            ignoreException(msg);
        }
    }

    public void ignoreException(String message) {
        exceptionTracker.ignoreException(message);
    }

    public void expectException(String message) {
        exceptionTracker.expectException(message);
    }

    public void ignoreException(Predicate<ExceptionKey> predicate, String description) {
        exceptionTracker.ignoreException(predicate, description);
    }

    public void expectException(Predicate<ExceptionKey> predicate, String description) {
        exceptionTracker.expectException(predicate, description);
    }

    public void checkExceptions() {
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
