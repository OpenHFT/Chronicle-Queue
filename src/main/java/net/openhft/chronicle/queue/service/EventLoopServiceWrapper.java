/*
 * Copyright 2016-2020 chronicle.software
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue.service;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.queue.ChronicleQueue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

@Deprecated /* For removal in x.22. This is now an internal concern. */
public class EventLoopServiceWrapper<O> implements ServiceWrapper, EventHandler {
    @NotNull
    protected final MethodReader[] serviceIn;
    private final HandlerPriority priority;
    @NotNull
    private final ChronicleQueue[] inputQueues;
    @NotNull
    private final ChronicleQueue outputQueue;
    @NotNull
    private final O serviceOut;
    private final boolean createdEventLoop;
    private final Object[] serviceImpl;
    private volatile boolean closed = false;
    @Nullable
    private EventLoop eventLoop;

    public EventLoopServiceWrapper(@NotNull ServiceWrapperBuilder<O> builder) {
        this.priority = builder.priority();
        outputQueue = ChronicleQueue.singleBuilder(builder.outputPath())
                .sourceId(builder.outputSourceId())
                .checkInterrupts(false)
                .build();
        serviceOut = outputQueue.acquireAppender()
                .methodWriterBuilder(builder.outClass())
                .get();
        serviceImpl = builder.getServiceFunctions().stream()
                .map(f -> f.apply(serviceOut))
                .toArray();

        List<String> paths = builder.inputPath();
        serviceIn = new MethodReader[paths.size()];
        inputQueues = new ChronicleQueue[paths.size()];
        for (int i = 0; i < paths.size(); i++) {
            inputQueues[i] = ChronicleQueue.singleBuilder(paths.get(i))
                    .sourceId(builder.inputSourceId())
                    .build();
            serviceIn[i] = inputQueues[i].createTailer()
                    .afterLastWritten(outputQueue)
                    .methodReader(serviceImpl);
        }
        eventLoop = builder.eventLoop();
        eventLoop.addHandler(this);
        createdEventLoop = builder.createdEventLoop();
        if (createdEventLoop)
            eventLoop.start();
    }

    @NotNull
    @Override
    public ChronicleQueue[] inputQueues() {
        return inputQueues;
    }

    @NotNull
    @Override
    public ChronicleQueue outputQueue() {
        return outputQueue;
    }

    @Override
    public boolean action() throws InvalidEventHandlerException {
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

    @NotNull
    @Override
    public HandlerPriority priority() {
        return priority;
    }

    @Override
    public void close() {
        closed = true;
        EventLoop eventLoop = this.eventLoop;
        if (eventLoop != null) {
            eventLoop.unpause();
        }
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
