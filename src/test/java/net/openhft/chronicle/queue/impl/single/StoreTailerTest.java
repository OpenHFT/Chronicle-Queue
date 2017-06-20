package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.service.HelloWorld;
import net.openhft.chronicle.wire.MethodReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class StoreTailerTest {
    private static final Path BASE_PATH = Paths.get(OS.TARGET, StoreTailerTest.class.getSimpleName());

    private Path dataDirectory;
    private SingleChronicleQueue firstInputQueue;
    private SingleChronicleQueue secondInputQueue;
    private SingleChronicleQueue outputQueue;

    @Before
    public void before() throws Exception {
        dataDirectory = BASE_PATH.resolve(Paths.get(Long.toString(System.nanoTime())));
        firstInputQueue = SingleChronicleQueueBuilder.binary(dataDirectory.resolve(Paths.get("firstInputQueue")))
                .sourceId(1)
                .rollCycle(RollCycles.TEST_DAILY)
                .build();
        secondInputQueue = SingleChronicleQueueBuilder.binary(dataDirectory.resolve(Paths.get("secondInputQueue")))
                .sourceId(2)
                // different RollCycle means that indicies are not identical to firstInputQueue
                .rollCycle(RollCycles.TEST_SECONDLY)
                .build();
        outputQueue = SingleChronicleQueueBuilder.binary(dataDirectory.resolve(Paths.get("outputQueue")))
                .testBlockSize()
                .rollCycle(RollCycles.TEST_DAILY)
                .build();
    }

    @Test
    public void shouldConsiderSourceIdWhenDeterminingLastWrittenIndex() throws Exception {
        final StringEvents firstWriter = firstInputQueue.acquireAppender().
                methodWriterBuilder(StringEvents.class).get();
        final HelloWorld secondWriter = secondInputQueue.acquireAppender().
                methodWriterBuilder(HelloWorld.class).get();

        // generate some data in the input queues
        firstWriter.onEvent("one");
        firstWriter.onEvent("two");

        secondWriter.hello("thirteen");
        secondWriter.hello("thirtyOne");

        final StringEvents eventSink = outputQueue.acquireAppender().
                methodWriterBuilder(StringEvents.class).recordHistory(true).get();

        final CapturingStringEvents outputWriter = new CapturingStringEvents(eventSink);
        final MethodReader firstMethodReader = firstInputQueue.createTailer().methodReader(outputWriter);
        final MethodReader secondMethodReader = secondInputQueue.createTailer().methodReader(outputWriter);

        // replay events from the inputs into the output queue
        assertThat(firstMethodReader.readOne(), is(true));
        assertThat(firstMethodReader.readOne(), is(true));
        assertThat(secondMethodReader.readOne(), is(true));
        assertThat(secondMethodReader.readOne(), is(true));

        // ensures that tailer is not moved to index from the incorrect source
        secondInputQueue.createTailer().afterLastWritten(outputQueue);
    }

    @After
    public void after() throws Exception {
        closeQueues(firstInputQueue, secondInputQueue, outputQueue);
        IOTools.deleteDirWithFiles(dataDirectory.toFile(), 10);
    }

    public interface StringEvents {
        void onEvent(final String event);
    }

    private static final class CapturingStringEvents implements StringEvents {
        private final StringEvents delegate;

        CapturingStringEvents(final StringEvents delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onEvent(final String event) {
            delegate.onEvent(event);
        }
    }

    private static void closeQueues(final SingleChronicleQueue... queues) {
        for (SingleChronicleQueue queue : queues) {
            if (queue != null) {
                queue.close();
            }
        }
    }
}