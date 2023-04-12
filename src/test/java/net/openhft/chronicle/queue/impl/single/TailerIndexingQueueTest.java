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

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.util.stream.IntStream.range;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

public final class TailerIndexingQueueTest extends QueueTestCommon {
    private final File path = getTmpDir();
    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());

    private static void deleteFile(final Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new AssertionError("Could not delete", e);
        }
    }

    private static ChronicleQueue createQueue(final File path, final TimeProvider timeProvider) {
        return SingleChronicleQueueBuilder.
                binary(path).
                timeProvider(timeProvider).
                rollCycle(TEST_SECONDLY).
                testBlockSize().
                wireType(WireType.BINARY).
                build();
    }

    @Test
    public void tailerShouldBeAbleToMoveBackwardFromEndOfCycle() throws IOException {
        assumeFalse(OS.isWindows());
        try (final ChronicleQueue queue = createQueue(path, clock::get)) {
            final ExcerptAppender appender = queue.acquireAppender();
            // generate some cycle files
            range(0, 5).forEach(i -> {
                try (final DocumentContext ctx = appender.writingDocument()) {
                    ctx.wire().write().int32(i);
                    clock.addAndGet(TimeUnit.SECONDS.toMillis(10L));
                }
            });
        }

        // remove all but the first file
        try (Stream<Path> list = Files.list(this.path.toPath());
             Stream<Path> list2 = Files.list(this.path.toPath())) {
            final Path firstFile =
                    list.sorted(Comparator.comparing(Path::toString))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            list2.filter(p -> !p.equals(firstFile))
                    .forEach(TailerIndexingQueueTest::deleteFile);

            try (final ChronicleQueue queue = createQueue(path, SystemTimeProvider.INSTANCE)) {
                final ExcerptTailer tailer = queue.createTailer().toEnd();
                // move to END_OF_CYCLE
                try (final DocumentContext readCtx = tailer.readingDocument()) {
                    assertFalse(readCtx.isPresent());
                }
                assertEquals(TailerState.END_OF_CYCLE, tailer.state());

                tailer.direction(TailerDirection.BACKWARD);

                tailer.toEnd();
                assertTrue(tailer.readingDocument().isPresent());
            }
        }
    }
}