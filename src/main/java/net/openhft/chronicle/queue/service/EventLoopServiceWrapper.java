package net.openhft.chronicle.queue.service;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.MethodReader;

import java.util.List;

/**
 * Created by peter on 01/04/16.
 */
public class EventLoopServiceWrapper<O> implements ServiceWrapper, EventHandler {
    protected final MethodReader[] serviceIn;
    private final HandlerPriority priority;
    private final ChronicleQueue[] inputQueues;
    private final ChronicleQueue outputQueue;
    private final O serviceOut;
    private final boolean createdEventLoop;
    private final Object[] serviceImpl;
    private volatile boolean closed = false;
    private EventLoop eventLoop;

    public EventLoopServiceWrapper(ServiceWrapperBuilder<O> builder) {
        this.priority = builder.priority();
        outputQueue = SingleChronicleQueueBuilder.binary(builder.outputPath()).sourceId(builder.outputSourceId()).build();
        serviceOut = outputQueue.createAppender().methodWriterBuilder(builder.outClass()).recordHistory(builder.outputSourceId() != 0).get();
        serviceImpl = builder.getServiceFunctions().stream().map(f -> f.apply(serviceOut)).toArray();

        List<String> paths = builder.inputPath();
        serviceIn = new MethodReader[paths.size()];
        inputQueues = new ChronicleQueue[paths.size()];
        for (int i = 0; i < paths.size(); i++) {
            inputQueues[i] = SingleChronicleQueueBuilder.binary(paths.get(i)).sourceId(builder.inputSourceId()).build();
            serviceIn[i] = inputQueues[i].createTailer().afterLastWritten(outputQueue).methodReader(serviceImpl);
        }
        eventLoop = builder.eventLoop();
        eventLoop.addHandler(this);
        createdEventLoop = builder.createdEventLoop();
        if (createdEventLoop)
            eventLoop.start();
    }

    @Override
    public ChronicleQueue[] inputQueues() {
        return inputQueues;
    }

    @Override
    public ChronicleQueue outputQueue() {
        return outputQueue;
    }

    @Override
    public boolean action() throws InvalidEventHandlerException, InterruptedException {
        if (isClosed()) {
            Closeable.closeQuietly(serviceImpl);
            Closeable.closeQuietly(serviceIn);
            Closeable.closeQuietly(outputQueue);
            Closeable.closeQuietly(inputQueues);
            throw new InvalidEventHandlerException();
        }

        boolean busy = false;
        for (MethodReader reader : serviceIn) {
            busy |= reader.readOne();
        }
        return busy;
    }

    @Override
    public HandlerPriority priority() {
        return priority;
    }

    @Override
    public void close() {
        closed = true;
        EventLoop eventLoop = this.eventLoop;
        this.eventLoop = null;
        if (createdEventLoop && eventLoop != null) {
            eventLoop.close();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
