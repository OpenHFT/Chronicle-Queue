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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import static org.junit.Assert.fail;

public final class FileModificationTimeTest extends QueueTestCommon {
    private final AtomicInteger fileCount = new AtomicInteger();

    private static void waitForDiff(final long a, final LongSupplier b) {
        final long timeout = System.currentTimeMillis() + 15_000L;
        while ((!Thread.currentThread().isInterrupted()) && System.currentTimeMillis() < timeout) {
            if (a != b.getAsLong()) {
                return;
            }
            Jvm.pause(1_000L);
        }

        fail("Values did not become different");
    }

    @Test
    public void shouldUpdateDirectoryModificationTime() {
        final File dir = getTmpDir();
        dir.mkdirs();

        final long startModTime = dir.lastModified();

        modifyDirectoryContentsUntilVisible(dir, startModTime);

        final long afterOneFile = dir.lastModified();

        modifyDirectoryContentsUntilVisible(dir, afterOneFile);
    }

    private void modifyDirectoryContentsUntilVisible(final File dir, final long startTime) {
        waitForDiff(startTime, () -> {
            createFile(dir, fileCount.getAndIncrement() + ".txt");
            return dir.lastModified();
        });
    }

    private void createFile(
            final File dir, final String filename) {
        final File file = new File(dir, filename);
        try (final FileWriter writer = new FileWriter(file)) {

            writer.append("foo");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
