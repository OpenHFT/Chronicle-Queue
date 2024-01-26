package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.testframework.process.JavaProcessBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A named tailer should not persist its index in the middle of a read operation, only when the read has succeeded.
 * This test demonstrates that:
 * <ul>
 *     <li>For normal tailers the index is not persisted unless the context was properly closed</li>
 *     <li>For method readers we expect that if there is a failure during the read then the index advancement should not be persisted</li>
 * </ul>
 */
public class NamedTailerFaultToleranceTest extends QueueTestCommon {

    private static final Logger log = LoggerFactory.getLogger(NamedTailerFaultToleranceTest.class);
    private static final File QUEUE_PATH = Paths.get(OS.getTmp(), NamedTailerFaultToleranceTest.class.getSimpleName()).toFile();
    private static final String TAILER_NAME = "test";

    @BeforeEach
    @AfterEach
    public void cleanupQueueData() {
        IOTools.deleteDirWithFiles(QUEUE_PATH);
    }

    @Test
    public void namedTailerIndexSemantics_start() {
        try (ChronicleQueue queue = createQueueInstance(); ExcerptAppender appender = queue.createAppender()) {
            assertNamedTailerIndex(0);
        }
    }

    @Test
    public void namedTailerIndexSemantics_readOnce() {
        try (ChronicleQueue queue = createQueueInstance();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer(TAILER_NAME)) {
            appender.writeText("a");
            assertNamedTailerIndex(0);
            tailer.readText();
            assertNamedTailerIndex(1);
        }
    }

    @Test
    public void namedTailerIndexShouldNotBePersistedMidRead_regularTailer() throws InterruptedException {
        seedInitialQueueData();
        assertNamedTailerIndex(0);
        Process start = JavaProcessBuilder.create(RegularTailerProcess.class).inheritingIO().start();
        start.waitFor(10, TimeUnit.SECONDS);
        assertNamedTailerIndex(1);
    }

    /**
     * This test creates a method writer and reader. The queue is populated with method writer invocations and then a
     * separate process is started that will use a method reader to read 2 messages. The method reader will call System
     * exit on its second invocation.
     */
    @Test
    public void namedTailerIndexShouldNotBePersistedMidRead_methodWriter() throws InterruptedException {
        try (ChronicleQueue queue = createQueueInstance(); ExcerptAppender appender = queue.createAppender()) {
            StringConsumer consumer = appender.methodWriter(StringConsumer.class);
            for (int i = 0; i < 10; i++) {
                String data = "message_" + i;
                consumer.accept(data);
                log.info("Appended: index={}, data={}", queue.lastIndex(), data);
            }
        }

        assertNamedTailerIndex(0);
        Process start = JavaProcessBuilder.create(MethodReaderTailerProcess.class).inheritingIO().start();
        start.waitFor(10, TimeUnit.SECONDS);
        assertNamedTailerIndex(1);
    }

    private static void assertNamedTailerIndex(long expected) {
        try (ChronicleQueue queue = createQueueInstance(); LongValue indexValue = queue.indexForId(TAILER_NAME)) {
            long index = indexValue.getVolatileValue();
            log.info("Actual named tailer index={}", index);
            assertEquals(expected, index);
        }
    }

    private static void seedInitialQueueData() {
        // Seed initial data
        try (ChronicleQueue queue = createQueueInstance(); ExcerptAppender appender = queue.createAppender()) {
            for (int i = 0; i < 10; i++) {
                String data = "message_" + i;
                appender.writeText(data);
                log.info("Appended: index={}, data={}", appender.lastIndexAppended(), data);
            }
        }
    }

    public static ChronicleQueue createQueueInstance() {
        return SingleChronicleQueueBuilder.builder().timeProvider(new SetTimeProvider()).path(QUEUE_PATH).build();
    }

    public static class RegularTailerProcess {
        public static void main(String[] args) {
            log.info("Starting [in separate process pid={}]", Jvm.getProcessId());
            try (ChronicleQueue queue = createQueueInstance(); ExcerptTailer tailer = queue.createTailer(TAILER_NAME)) {
                try (DocumentContext context = tailer.readingDocument()) {
                    // Intentional no-op
                    log.info("Consuming first document");
                }
                try (DocumentContext context = tailer.readingDocument()) {
                    log.info("Consuming second document but exiting while the context is open. index={}", context.index());
                    System.exit(-1);
                }
            }
        }
    }

    public interface StringConsumer {
        void accept(String value);
    }

    public static class MethodReaderTailerProcess {

        private static final Logger log = LoggerFactory.getLogger(MethodReaderTailerProcess.class);

        public static void main(String[] args) {
            log.info("Starting [in separate process pid={}]", Jvm.getProcessId());
            AtomicInteger counter = new AtomicInteger(0);

            StringConsumer consumer = (s) -> {
                log.info("Consuming, data={}", s);
                if (counter.get() >= 1) {
                    log.info("Calling System.exit(-1) [in separate process pid={}]", Jvm.getProcessId());
                    System.exit(-1);
                }
                counter.incrementAndGet();
            };

            try (ChronicleQueue queue = createQueueInstance();
                 ExcerptTailer tailer = queue.createTailer(TAILER_NAME);
            ) {
                MethodReader methodReader = tailer.methodReader(consumer);
                methodReader.readOne();
                counter.incrementAndGet();
                methodReader.readOne();
            }
        }
    }

}
