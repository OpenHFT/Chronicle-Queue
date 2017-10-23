/*
 * Copyright 2014-2017 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.reader.ChronicleReader;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class RollEOFTest {

    private final File path = DirectoryUtils.tempDir(getClass().getName());

    @Test(timeout = 5000L)
    public void testRollWritesEOF() throws Exception {

        final MutableTimeProvider timeProvider = new MutableTimeProvider();
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_MONTH, -1);
        timeProvider.setTime(cal.getTimeInMillis());
        createQueueAndWriteData(timeProvider);
        assertEquals(1, getNumberOfQueueFiles());

        // adjust time
        timeProvider.setTime(System.currentTimeMillis());
        createQueueAndWriteData(timeProvider);
        assertEquals(2, getNumberOfQueueFiles());

        List<String> l = new LinkedList<>();
        new ChronicleReader().withMessageSink(l::add).withBasePath(path.toPath()).execute();
        // 2 entries per message
        assertEquals(4, l.size());
    }

    private long getNumberOfQueueFiles() throws IOException {
        return getQueueFilesStream().count();
    }

    private Stream<Path> getQueueFilesStream() throws IOException {
        return Files.list(path.toPath()).filter(p -> p.toString().endsWith(SingleChronicleQueue.SUFFIX));
    }

    private void createQueueAndWriteData(MutableTimeProvider timeProvider) {
        final SingleChronicleQueue queue = SingleChronicleQueueBuilder
                .binary(path)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_DAILY)
                .timeProvider(timeProvider)
                .build();

        ExcerptAppender excerptAppender = queue.acquireAppender();

        try(DocumentContext dc = excerptAppender.writingDocument(false)) {
            dc.wire().write(() -> "test").int64(0);
        }
    }

    private static final class MutableTimeProvider implements TimeProvider {
        private long currentTimeMillis;

        @Override
        public long currentTimeMillis() {
            return currentTimeMillis;
        }

        void setTime(final long millis) {
            this.currentTimeMillis = millis;
        }

        void addTime(final long duration, final TimeUnit unit) {
            this.currentTimeMillis += unit.toMillis(duration);
        }
    }
}
