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

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.*;

/** Java-only harness controls; these do not claim C++ interoperability. */
public class RawAccessJavaFixtureTest extends QueueTestCommon {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void javaFixtureRoundTripChecksAllRecordsAndBoundaries() {
        File path = fixturePath();
        RawAccessJavaTest.writeFixture(path, RawAccessJavaTest.COUNT);
        RawAccessJavaTest.readFixture(path, RawAccessJavaTest.COUNT);
    }

    @Test
    public void missingFixtureFailsBeforeOpeningAQueue() {
        AssertionError failure = assertThrows(AssertionError.class,
                () -> RawAccessJavaTest.readFixture(fixturePath(), RawAccessJavaTest.COUNT));
        assertTrue(failure.getMessage().contains("Missing C++ fixture directory"));
    }

    @Test
    public void incompleteFixtureReportsTheMissingRecord() {
        File path = fixturePath();
        RawAccessJavaTest.writeFixture(path, RawAccessJavaTest.COUNT - 1);
        AssertionError failure = assertThrows(AssertionError.class,
                () -> RawAccessJavaTest.readFixture(path, RawAccessJavaTest.COUNT));
        assertTrue(failure.getMessage().contains("missing record 10 of 10"));
    }

    @Test
    public void extraRecordIsRejected() {
        File path = fixturePath();
        RawAccessJavaTest.writeFixture(path, RawAccessJavaTest.COUNT + 1);
        AssertionError failure = assertThrows(AssertionError.class,
                () -> RawAccessJavaTest.readFixture(path, RawAccessJavaTest.COUNT));
        assertTrue(failure.getMessage().contains("more than 10 records"));
    }

    @Test
    public void obsoleteInnerLengthPrefixIsRejected() {
        File path = fixturePath();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).build();
             ExcerptAppender appender = queue.acquireAppender();
             DocumentContext document = appender.writingDocument()) {
            document.wire().bytes().writeInt(RawAccessJavaTest.PAYLOAD_LENGTH);
            RawAccessJavaTest.writePayload(document.wire().bytes());
        }
        AssertionError failure = assertThrows(AssertionError.class,
                () -> RawAccessJavaTest.readFixture(path, 1));
        assertTrue(failure.getMessage().contains("do not add an inner length prefix"));
    }

    private File fixturePath() {
        return new File(temporary.getRoot(), "queue");
    }
}
