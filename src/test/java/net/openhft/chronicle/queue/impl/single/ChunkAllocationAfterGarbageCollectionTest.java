package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.NewChunkListener;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.PretouchHandler;
import net.openhft.chronicle.queue.service.HelloReplier;
import net.openhft.chronicle.queue.service.HelloWorld;
import net.openhft.chronicle.queue.service.HelloWorldImpl;
import net.openhft.chronicle.queue.service.ServiceWrapper;
import net.openhft.chronicle.queue.service.ServiceWrapperBuilder;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.wire.MethodReader;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertTrue;

public final class ChunkAllocationAfterGarbageCollectionTest {
    private static int MONITOR_LOOP_DELAY;
    private static final byte[] DATA = new byte[8192];

    @BeforeClass
    public static void decreaseMonitorLoopDelay() {
        MONITOR_LOOP_DELAY = Integer.getInteger("MonitorInitialDelay", 60_000);
        System.setProperty("MonitorInitialDelay", "0");
    }

    @AfterClass
    public static void resetMonitorLoopDelay() {
        System.setProperty("MonitorInitialDelay", Integer.toString(MONITOR_LOOP_DELAY));
    }

    @Test
    public void shouldNotReallocateAllChunksAfterGarbageCollection() throws Exception {
        final String input = tempDir("input").getAbsolutePath();
        final String output = tempDir("output").getAbsolutePath();
        final ServiceWrapperBuilder<HelloReplier> builder =
                ServiceWrapperBuilder.serviceBuilder(input,
                        output, HelloReplier.class, HelloWorldImpl::new).
                        inputSourceId(1).outputSourceId(2);

        HelloWorld helloWorld = builder.inputWriter(HelloWorld.class);

        final CapturingHelloReplier capturingHelloReplier = new CapturingHelloReplier();
        MethodReader replyReader = builder.outputReader(capturingHelloReplier);
        final EventGroup eventLoop = new EventGroup(false);
        builder.eventLoop(eventLoop);

        ServiceWrapper helloWorldService = builder.get();
        final CountingNewChunkListener outputQueueChunkListener = new CountingNewChunkListener();
        final SingleChronicleQueue outputQueue = (SingleChronicleQueue) helloWorldService.outputQueue();
        outputQueue.storeForCycle(outputQueue.cycle(), 0, true).bytes().
                setNewChunkListener(outputQueueChunkListener);

        eventLoop.start();

        final CountingEventHandlerWrapper pretouchWrapper = new CountingEventHandlerWrapper(new PretouchHandler(outputQueue));
        eventLoop.addHandler(pretouchWrapper);

        pretouchWrapper.enabled.set(true);
        waitForPretoucherToBeInvoked(pretouchWrapper);
        pretouchWrapper.enabled.set(false);

        range(0, 5_000).forEach(i -> {
            helloWorld.hello(new String(DATA));
            helloWorld.hello(new String(DATA));

            if (i % 1000 == 0) {
                // give event loop monitor time to run
                waitForPretoucherToBeInvoked(pretouchWrapper);
            }
        });

        for (int i = 0; i < 5_000; i++) {
            while (!replyReader.readOne()) {
                Thread.yield();
            }
        }

        pretouchWrapper.enabled.set(true);
        waitForPretoucherToBeInvoked(pretouchWrapper);

        while (true) {
            int last = outputQueueChunkListener.getLastObservedChunk();
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50L));
            if (last == outputQueueChunkListener.getLastObservedChunk()) {
                break;
            }
        }

        final int lastObservedAllocatedChunk = outputQueueChunkListener.getLastObservedChunk();
        outputQueueChunkListener.resetObservations();

        GcControls.waitForGcCycle();
        capturingHelloReplier.messages.clear();
        GcControls.waitForGcCycle();

        range(0, 5_000).forEach(i -> {
            helloWorld.hello(new String(DATA));
            helloWorld.hello(new String(DATA));

            if (i % 1000 == 0) {
                // give event loop monitor time to run
                waitForPretoucherToBeInvoked(pretouchWrapper);
            }
        });

        assertTrue(outputQueueChunkListener.getLastObservedChunk() > lastObservedAllocatedChunk);
        assertTrue(outputQueueChunkListener.getLowestObservedChunk() > lastObservedAllocatedChunk);
    }

    private void waitForPretoucherToBeInvoked(final CountingEventHandlerWrapper pretouchWrapper) {
        long pretouchInvocationCount = pretouchWrapper.invocationCount.get();
        while (pretouchWrapper.invocationCount.get() == pretouchInvocationCount) {
            Thread.yield();
        }
    }

    private static final class CountingEventHandlerWrapper implements EventHandler {
        private final EventHandler delegate;
        private final AtomicLong invocationCount = new AtomicLong();
        private final AtomicBoolean enabled = new AtomicBoolean(false);

        private CountingEventHandlerWrapper(final EventHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void eventLoop(final EventLoop eventLoop) {
            delegate.eventLoop(eventLoop);
        }

        @Override
        @NotNull
        public HandlerPriority priority() {
            return delegate.priority();
        }

        @Override
        public boolean action() throws InvalidEventHandlerException, InterruptedException {
            invocationCount.incrementAndGet();
            return enabled.get() && delegate.action();
        }
    }


    @NotNull
    private static File tempDir(final String suffix) {
        return DirectoryUtils.tempDir(ChunkAllocationAfterGarbageCollectionTest.class.getSimpleName() + suffix);
    }

    private static final class CountingNewChunkListener implements NewChunkListener {
        private final AtomicInteger lowestObservedChunk = new AtomicInteger(Integer.MIN_VALUE);
        private final AtomicInteger lastObservedChunk = new AtomicInteger();

        @Override
        public void onNewChunk(final String filename, final int chunk, final long delayMicros) {
            if (chunk > Integer.MIN_VALUE || chunk < lowestObservedChunk.get()) {
                lowestObservedChunk.set(chunk);
            }
            lastObservedChunk.set(chunk);
        }

        int getLowestObservedChunk() {
            return lowestObservedChunk.get();
        }

        int getLastObservedChunk() {
            return lastObservedChunk.get();
        }

        void resetObservations() {
            lowestObservedChunk.set(Integer.MIN_VALUE);
            lastObservedChunk.set(Integer.MIN_VALUE);
        }
    }

    private static final class CapturingHelloReplier implements HelloReplier {
        private List<String> messages = new CopyOnWriteArrayList<>();

        @Override
        public void reply(final String message) {
            messages.add(message);
        }
    }
}