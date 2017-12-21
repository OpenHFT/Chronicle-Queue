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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.bytes.ref.BinaryLongArrayReference;
import net.openhft.chronicle.bytes.ref.BinaryLongReference;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class NotCompleteTest {

    private final boolean lazyIndexing;

    public NotCompleteTest(final String type, boolean lazyIndexing) {
        this.lazyIndexing = lazyIndexing;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"eager", false},
                {"lazy", true}
        });
    }

    /**
     * tests that when flags are set to not complete we are able to recover
     */
    @Test
    public void testUsingANotCompleteQueue()
            throws TimeoutException, ExecutionException, InterruptedException {

        BinaryLongReference.startCollecting();

        File tmpDir = DirectoryUtils.tempDir("testUsingANotCompleteQueue");
        try (final ChronicleQueue queue = binary(tmpDir)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_DAILY)
                .build()) {

            ExcerptAppender appender = queue.acquireAppender()
                    .lazyIndexing(lazyIndexing);

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("some").text("data");
            }

            Thread.sleep(100);

//            System.out.println(queue.dump());

            // this is what will corrupt the queue
            BinaryLongReference.forceAllToNotCompleteState();
        }
        try (final ChronicleQueue queue = binary(tmpDir)
                .testBlockSize()
                .timeoutMS(500)
                .build()) {
//            System.out.println(queue.dump());

            ExcerptTailer tailer = queue.createTailer();

            try (DocumentContext dc = tailer.readingDocument()) {
                assertEquals("data", dc.wire().read(() -> "some").text());
            }
        }
    }

    @Test
    public void testUsingANotCompleteArrayQueue()
            throws TimeoutException, ExecutionException, InterruptedException {

        BinaryLongArrayReference.startCollecting();

        File tmpDir = DirectoryUtils.tempDir("testUsingANotCompleteArrayQueue");
        try (final ChronicleQueue queue = binary(tmpDir)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_DAILY)
                .build()) {

            ExcerptAppender appender = queue.acquireAppender()
                    .lazyIndexing(lazyIndexing);

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("some").text("data");
            }

            Thread.sleep(100);

//            System.out.println(queue.dump());

            // this is what will corrupt the queue
            BinaryLongArrayReference.forceAllToNotCompleteState();
        }
        try (final ChronicleQueue queue = binary(tmpDir)
                .testBlockSize()
                .timeoutMS(500)
                .build()) {
//            System.out.println(queue.dump());

            ExcerptTailer tailer = queue.createTailer();

            try (DocumentContext dc = tailer.readingDocument()) {
                assertEquals("data", dc.wire().read(() -> "some").text());
            }
        }
    }

    @Test
    public void testMessageLeftNotComplete()
            throws TimeoutException, ExecutionException, InterruptedException {

        File tmpDir = DirectoryUtils.tempDir("testMessageLeftNotComplete");
        try (final ChronicleQueue queue = binary(tmpDir).testBlockSize().rollCycle(RollCycles.TEST_DAILY).build()) {
            ExcerptAppender appender = queue.acquireAppender()
                    .lazyIndexing(lazyIndexing);

            // start a message which was not completed.
            DocumentContext dc = appender.writingDocument();
            dc.wire().write("some").text("data");
            // didn't call dc.close();
        }

        final SingleChronicleQueue singleChronicleQueue = null;
        try (final ChronicleQueue queue = binary(tmpDir).testBlockSize().build()) {
            ExcerptTailer tailer = queue.createTailer();

            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent());
            }

            String expectedEager = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    0,\n" +
                    "    0\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 401,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 401, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  504,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 504, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 0\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 600, header: -1 or 0\n" +
                    "--- !!not-ready-data! #binary\n" +
                    "...\n" +
                    "# 130468 bytes remaining\n";
            String expectedLazy = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    0,\n" +
                    "    0\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 401, header: -1 or 0\n" +
                    "--- !!not-ready-data! #binary\n" +
                    "...\n" +
                    "# 130667 bytes remaining\n";
            assertEquals(lazyIndexing ? expectedLazy : expectedEager, queue.dump());
        }

        try (final ChronicleQueue queue = binary(tmpDir).testBlockSize().timeoutMS(500).build()) {
            ExcerptAppender appender = queue.acquireAppender();

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("some").text("data");
            }

            String expectedEager = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    33372,\n" +
                    "    143331648602112\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 401,\n" +
                    "    lastIndex: 1\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 401, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  504,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 504, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  33372,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 600, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "\"!! Skipped due to recovery of locked header !!\"\n" +
                    "# position: 33372, header: 0\n" +
                    "--- !!data #binary\n" +
                    "some: data\n" +
                    "...\n" +
                    "# 97682 bytes remaining\n";
            String expectedLazy = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    33368,\n" +
                    "    143314468732928\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 33172,\n" +
                    "    lastIndex: 1\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 401, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "\"!! Skipped due to recovery of locked header !!\"\n" +
                    "# position: 33172, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  33272,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 33272, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  33368,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 33368, header: 0\n" +
                    "--- !!data #binary\n" +
                    "some: data\n" +
                    "...\n" +
                    "# 97686 bytes remaining\n";
            assertEquals(lazyIndexing ? expectedLazy : expectedEager, queue.dump());
        }
    }

    @Test
    public void testInterruptedDuringSerialisation()
            throws TimeoutException, ExecutionException, InterruptedException {

        final File tmpDir = DirectoryUtils.tempDir("testInterruptedDuringSerialisation_"+(lazyIndexing?"lazy":"eager"));
        DirectoryUtils.deleteDir(tmpDir);
        tmpDir.mkdirs();

        final List<String> names = Collections.synchronizedList(new ArrayList<>());
        final Person person1 = new Person(40, "Terry");
        final Person interrupter = new Person(50, Person.INTERRUPT);
        final Person thrower = new Person(80, Person.THROW);
        final Person person2 = new Person(90, "Bert");

        try (final ChronicleQueue queueReader = binary(tmpDir)
                .testBlockSize()
                .timeoutMS(500)
                .build()) {

            ExcerptTailer tailer = queueReader.createTailer();
            MethodReader reader = tailer.methodReader((PersonListener) person -> names.add(person.name));

            final StringBuilder queueDumpBeforeInterruptedWrite = new StringBuilder();
            // set up
            doWrite(tmpDir, (proxy, queue) -> {
                proxy.accept(person1);
                queueDumpBeforeInterruptedWrite.append(queue.dump());
            });
            final String cleanedQueueDump = cleanQueueDump(queueDumpBeforeInterruptedWrite.toString());

            // start up writer thread
            Thread writerThread = new Thread(() -> doWrite(tmpDir, (proxy, queue) -> {
                // thread is interrupted during this
                proxy.accept(interrupter);
            }));
            writerThread.start();
            writerThread.join();

            try (final ChronicleQueue queue = binary(tmpDir)
                    .testBlockSize()
                    .rollCycle(RollCycles.TEST_DAILY)
                    .build()) {
                String dump = cleanQueueDump(queue.dump());
                assertEquals("queue should be unchanged by the interrupted write", cleanedQueueDump, dump);
            }

            // check only 1 written
            assertTrue(reader.readOne());
            assertEquals(1, names.size());
            assertEquals(person1.name, names.get(0));
            assertFalse(reader.readOne());

            // do a write that throws an exception
            doWrite(tmpDir, (proxy, queue) -> {
                try {
                    proxy.accept(thrower);
                } catch (NullPointerException npe) {
                    // ignore
                }
            });

            try (final ChronicleQueue queue = binary(tmpDir)
                    .testBlockSize()
                    .rollCycle(RollCycles.TEST_DAILY)
                    .build()) {
                String dump = cleanQueueDump(queue.dump());
                if (lazyIndexing) {
                    // reading the queue creates the index, thus changing it, so do a text comparison here
                    assertEquals("queue should be unchanged by the failed write",
                            "--- !!meta-data #binary\n" +
                            "header: !SCQStore {\n" +
                            "  wireType: !WireType BINARY_LIGHT,\n" +
                            "  writePosition: [\n" +
                            "    401,\n" +
                            "    0\n" +
                            "  ],\n" +
                            "  roll: !SCQSRoll {\n" +
                            "    length: !int 86400000,\n" +
                            "    format: yyyyMMdd,\n" +
                            "    epoch: 0\n" +
                            "  },\n" +
                            "  indexing: !SCQSIndexing {\n" +
                            "    indexCount: 8,\n" +
                            "    indexSpacing: 1,\n" +
                            "    index2Index: 434,\n" +
                            "    lastIndex: 0\n" +
                            "  },\n" +
                            "  lastAcknowledgedIndexReplicated: -1,\n" +
                            "  recovery: !TimedStoreRecovery {\n" +
                            "    timeStamp: 0\n" +
                            "  },\n" +
                            "  deltaCheckpointInterval: 0\n" +
                            "}\n" +
                            "# position: 401, header: 0\n" +
                            "--- !!data #binary\n" +
                            "accept: {\n" +
                            "  age: 40,\n" +
                            "  name: Terry\n" +
                            "}\n" +
                            "# position: 434, header: 0\n" +
                            "--- !!meta-data #binary\n" +
                            "index2index: [\n" +
                            "  # length: 8, used: 0\n" +
                            "  0, 0, 0, 0, 0, 0, 0, 0\n" +
                            "]\n" +
                            "...\n" +
                            "\n", dump);
                }
                else
                    assertEquals("queue should be unchanged by the failed write", cleanedQueueDump, dump);
            }

            // check nothing else written
            assertFalse(reader.readOne());

            // write another person to same queue in this thread
            doWrite(tmpDir, (proxy, queue) -> proxy.accept(person2));

            assertTrue(reader.readOne());
            assertEquals(2, names.size());
            assertEquals(person2.name, names.get(1));
            assertFalse(reader.readOne());
        }
    }

    // the last line of the dump changes - haven't spent the time to get to the bottom of this
    private String cleanQueueDump(String from) {
        return from.replaceAll("# [0-9]+ bytes remaining$", "");
    }

    private void doWrite(File tmpDir, BiConsumer<PersonListener, ChronicleQueue> action) {
        try (final ChronicleQueue queue = binary(tmpDir)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_DAILY)
                .build()) {

            ExcerptAppender appender = queue.acquireAppender().lazyIndexing(lazyIndexing);
            PersonListener proxy = appender.methodWriterBuilder(PersonListener.class).get();
            action.accept(proxy, queue);
        }
    }

    @Ignore("discuss with Rob")
    @Test
    public void testSkipSafeLengthOverBlock()
            throws TimeoutException, ExecutionException, InterruptedException {

        File tmpDir = DirectoryUtils.tempDir("testSkipSafeLengthOverBlock");
        // 3rd time will do it
        for (int i=0; i<8; i++) {
            try (final ChronicleQueue queue = binary(tmpDir).testBlockSize().rollCycle(RollCycles.TEST_DAILY).timeoutMS(1).build()) {
                ExcerptAppender appender = queue.acquireAppender().lazyIndexing(lazyIndexing);
                // start a message which won't be completed.
                DocumentContext dc = appender.writingDocument();
                // 2nd and subsequent times we call this will invoke recovery
                dc.wire().write("some").text("data");
                // don't call dc.close();
            }
        }

        try (final ChronicleQueue queue = binary(tmpDir).testBlockSize().build()) {
            ExcerptTailer tailer = queue.createTailer();

            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent());
            }
       }
    }

    @After
    public void checkMappedFiles() {
        MappedFile.checkMappedFiles();
    }

    private interface PersonListener {
        void accept(Person name);
    }

    private class Person implements Marshallable {
        static final String INTERRUPT = "Arthur";
        static final String THROW = "Thrower";
        final int age;
        final String name;

        public Person(int age, String name) {
            this.age = age;
            this.name = name;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write("age").int32(age);
            // interrupt half way through writing
            if (INTERRUPT.equals(name)) {
                Thread.currentThread().interrupt();
            } else if (THROW.equals(name)) {
                throw new NullPointerException();
            } else {
                wire.write("name").text(name);
            }
        }

        @Override
        public String toString() {
            return "Person{" +
                    "age=" + age +
                    ", name='" + name + '\'' +
                    '}';
        }
    }
}