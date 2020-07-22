/*
 * Copyright 2016-2020 Chronicle Software
 *
 * https://chronicle.software
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
import net.openhft.chronicle.core.io.IOTools;
import org.jetbrains.annotations.NotNull;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.*;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ChronicleQueueTestBase extends QueueTestCommon {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ChronicleQueueTestBase.class);
    private static final boolean TRACE_TEST_EXECUTION = Jvm.getBoolean("queue.traceTestExecution");
    private List<File> tmpDirs = new ArrayList<>();

    static {
        System.setProperty("queue.check.index", "true");
    }

    // *************************************************************************
    // JUNIT Rules
    // *************************************************************************

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
                LOGGER.info("Starting test: {}.{}",
                        description.getClassName(),
                        description.getMethodName()
                );
            }
        }
    };

    // *************************************************************************
    //
    // *************************************************************************
    static AtomicLong counter = new AtomicLong();

    @BeforeClass
    public static void synchronousFileTruncating() {
        System.setProperty("chronicle.queue.synchronousFileShrinking", "true");
    }

    @NotNull
    protected File getTmpDir() {
        final String methodName = testName.getMethodName();
        String name = methodName == null ? "unknown" : methodName;
        final File tmpDir = DirectoryUtils.tempDir(name + "-" + counter.incrementAndGet());
        tmpDirs.add(tmpDir);
        return tmpDir;
    }

    @Override
    public void afterChecks() {
        super.afterChecks();

        // should be able to remove tmp dirs
        tmpDirs.forEach(file -> {
            if (file.exists() && !IOTools.deleteDirWithFiles(file)) {
                LOGGER.error("Could not delete tmp dir {}. Remaining {}", file, file.list());
            }
        });
    }
}
