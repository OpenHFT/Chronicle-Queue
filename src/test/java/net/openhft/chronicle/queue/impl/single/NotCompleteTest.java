/*
 * Copyright 2016-2020 chronicle.software
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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.threads.InterruptedRuntimeException;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.*;

/**
 * test for exceptions during serialisation out of messages, or for thread interrupts.
 * We want to ensure that messages are completely written or not written - no half measures.
 */
@RequiredForClient
public class NotCompleteTest extends ChronicleQueueTestBase {

    @Test
    public void testInterruptOrExceptionDuringSerialisation() throws InterruptedException {

        final File tmpDir = DirectoryUtils.tempDir("testInterruptedDuringSerialisation");
        try {
            DirectoryUtils.deleteDir(tmpDir);
            tmpDir.mkdirs();

            final List<String> names = Collections.synchronizedList(new ArrayList<>());
            final Person person1 = new Person(40, "Terry");
            final Person interrupter = new Person(50, Person.INTERRUPT);
            final Person thrower = new Person(80, Person.THROW);
            final Person person2 = new Person(90, "Bert");

            try (final ChronicleQueue queueReader = createQueue(tmpDir);
                 final ChronicleQueue queueWriter = createQueue(tmpDir)) {

                ExcerptTailer tailer = queueReader.createTailer();
                MethodReader reader = tailer.methodReader((PersonListener) person -> names.add(person.name));

                final StringBuilder queueDumpBeforeInterruptedWrite = new StringBuilder();
                // set up
                doWrite(queueWriter, (proxy, queue) -> {
                    proxy.accept(person1);
                    queueDumpBeforeInterruptedWrite.append(queue.dump());
                });
                String cleanedQueueDump = cleanQueueDump(queueDumpBeforeInterruptedWrite.toString());

                // start up writer thread
                Thread writerThread = new Thread(() -> doWrite(queueWriter, (proxy, queue) -> {
                    // thread is interrupted during this
                    try {
                        proxy.accept(interrupter);
                        Jvm.error().on(getClass(), "Should have interrupted");
                    } catch (InterruptedRuntimeException expected) {
                        // expected.
                    }
                }));
                writerThread.start();
                writerThread.join();

                try (final ChronicleQueue queue = createQueue(tmpDir)) {
                    String dump = cleanQueueDump(queue.dump());
                    assertEquals("queue should be unchanged by the interrupted write", cleanedQueueDump, dump);
                }

                // check only 1 written
                assertTrue(reader.readOne());
                assertEquals(1, names.size());
                assertEquals(person1.name, names.get(0));
                assertFalse(reader.readOne());

                // do a write that throws an exception
                doWrite(queueWriter, (proxy, queue) -> {
                    try {
                        proxy.accept(thrower);
                    } catch (NullPointerException npe) {
                        // ignore
                    }
                });

                try (final ChronicleQueue queue = createQueue(tmpDir)) {
                    String dump = cleanQueueDump(queue.dump());
                    assertEquals("queue should be unchanged by the failed (exception) write", cleanedQueueDump, dump);
//                    System.err.println(queue.dump());
                }

                // check nothing else written
                assertFalse(reader.readOne());

                // do an empty write
                ExcerptAppender appender = queueWriter.acquireAppender();
                DocumentContext wd = appender.writingDocument();
                wd.rollbackOnClose();
                wd.close();
                // check queue unchanged
                String dump = cleanQueueDump(queueWriter.dump());
                assertEquals("queue should be unchanged by the failed (rollback) write", cleanedQueueDump, dump);
                // check nothing else written
                assertFalse(reader.readOne());

                // write another person to same queue in this thread
                doWrite(queueWriter, (proxy, queue) -> proxy.accept(person2));

                assertTrue(reader.readOne());
                assertEquals(2, names.size());
                assertEquals(person2.name, names.get(1));
                assertFalse(reader.readOne());
            }
        } finally {
            try {
                IOTools.deleteDirWithFiles(tmpDir, 2);
            } catch (Exception e) {
                if (e instanceof AccessDeniedException && OS.isWindows())
                    return;
                throw e;
            }
        }
    }

    @NotNull
    private SingleChronicleQueue createQueue(File tmpDir) {
        return binary(tmpDir)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_DAILY)
                .timeoutMS(500)
                .checkInterrupts(true)
                .build();
    }

    // the last line of the dump changes - haven't spent the time to get to the bottom of this
    private String cleanQueueDump(String from) {
        return from.replaceAll("# [0-9]+ bytes remaining$", "").replaceAll("modCount: (\\d+)", "modCount: 00");
    }

    private void doWrite(ChronicleQueue queue, BiConsumer<PersonListener, ChronicleQueue> action) {
        ExcerptAppender appender = queue.acquireAppender();
        PersonListener proxy = appender.methodWriterBuilder(PersonListener.class).get();
        action.accept(proxy, queue);
    }

    @After
    public void clearInterrupt() {
        Thread.interrupted();
    }

    interface PersonListener {
        void accept(Person name);
    }

    static class Person implements Marshallable {
        static final String INTERRUPT = "Arthur";
        static final String THROW = "Thrower";
        final int age;
        final String name;

        Person(int age, String name) {
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