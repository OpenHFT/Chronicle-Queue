/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.WireType;
import org.junit.Rule;
import org.junit.rules.*;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedHashSet;
import java.util.Set;

public class ChronicleQueueTestBase {
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

    @Rule
    public TestRule watcher = new TestWatcher() {
        @Override
        protected void starting(Description description) {
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

    public static File getTmpDir() {
        try {
            final File tmpDir = Files.createTempDirectory("chronicle" + "-").toFile();

            DeleteStatic.INSTANCE.add(tmpDir);

            // Log the temporary directory in OSX as it is quite obscure
            if (OS.isMacOSX()) {
                LOGGER.info("Tmp dir: {}", tmpDir);
            }

            return tmpDir;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static RollingChronicleQueue createQueue(WireType type) {
        return new SingleChronicleQueueBuilder(getTmpDir()).wireType(type).build();
    }

    // *************************************************************************
    //
    // *************************************************************************

    private static void deleteDir(File dir) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDir(file);
                    } else if (!file.delete()) {
                        LOGGER.info("... unable to delete {}", file);
                    }
                }
            }
        }

        dir.delete();
    }

    protected void warmup(WireType type, int iterations) {
        ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(type)
                .blockSize(640_000)
                .build();

        ExcerptAppender appender = queue.acquireAppender();
        ExcerptTailer tailer = queue.createTailer();

        for (int i = 0; i < iterations; i++) {
            appender.writeDocument(w -> w.write(TestKey.test).text("warmup"));
        }

        for (int i = 0; i < iterations; i++) {
            tailer.readDocument(r -> r.read(TestKey.test).text());
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    public enum TestKey implements WireKey {
        test, test2
    }

    enum DeleteStatic {
        INSTANCE;
        final Set<File> toDeleteList = new LinkedHashSet<>();

        {
            Runtime.getRuntime().addShutdownHook(new Thread(
                () -> toDeleteList.forEach(ChronicleQueueTestBase::deleteDir)
            ));
        }

        synchronized void add(File path) {
            toDeleteList.add(path);
        }
    }
}
