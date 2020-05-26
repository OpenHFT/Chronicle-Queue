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

import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.wire.WireKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.*;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.UUID;

public class ChronicleQueueTestBase {
    static {
        System.setProperty("queue.check.index", "true");
    }
    protected static final Logger LOGGER = LoggerFactory.getLogger(ChronicleQueueTestBase.class);
    private static final boolean TRACE_TEST_EXECUTION = Boolean.getBoolean("queue.traceTestExecution");

    // *************************************************************************
    // JUNIT Rules
    // *************************************************************************

    @Rule
    public final TestName testName = new TestName();

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File(System.getProperty("java.io.tmpdir")));

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

    static void deleteDir(@NotNull String... dirs) {
        for (String dir : dirs) {
            try {
                deleteDir(new File(dir));
            } catch (Exception e) {
                Jvm.warn().on(ChronicleQueueTestBase.class, e);
            }
        }
    }

    public static void deleteDir(@NotNull File dir) {
        if (dir.isDirectory()) {
            @Nullable File[] files = dir.listFiles();
            if (files != null) {
                for (@NotNull File file : files) {
                    if (file.isDirectory()) {
                        deleteDir(file);
                    } else
                        //noinspection ResultOfMethodCallIgnored
                        file.delete();
                }
            }
        }

        dir.delete();
    }

    @NotNull
    protected File getTmpDir() {
        final String methodName = testName.getMethodName();
        return DirectoryUtils.tempDir(methodName != null ?
                methodName.replaceAll("[\\[\\]\\s]+", "_").replace(':', '_') : "NULL-" + UUID
                .randomUUID());
    }

    @BeforeClass
    public static void synchronousFileTruncating() {
        System.setProperty("chronicle.queue.synchronousFileShrinking", "true");
    }

    @After
    public void checkRegisteredBytes() {
        BytesUtil.checkRegisteredBytes();
    }

    public enum TestKey implements WireKey {
        test, test2
    }
}
