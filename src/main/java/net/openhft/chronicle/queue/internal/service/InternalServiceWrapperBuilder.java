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

package net.openhft.chronicle.queue.internal.service;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.service.EventLoopServiceWrapper;
import net.openhft.chronicle.queue.service.ServiceWrapper;
import net.openhft.chronicle.threads.EventGroup;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class InternalServiceWrapperBuilder<O> implements ServiceWrapper.Builder<O> {
    private final List<String> inputPaths = new ArrayList<>();
    private final List<Function<O, Object>> serviceFunctions = new ArrayList<>();
    private String outputPath;
    private Class<O> outClass;
    private EventLoop eventLoop;
    private HandlerPriority priority = HandlerPriority.MEDIUM;
    private boolean createdEventLoop = false;
    private int inputSourceId;
    private int outputSourceId;
    private final List<ChronicleQueue> queues = new ArrayList<>();

    InternalServiceWrapperBuilder() {
    }

    @NotNull
    public static <O> InternalServiceWrapperBuilder<O> serviceBuilder(String inputPath, String outputPath, Class<O> outClass, Function<O, Object> serviceFunction) {
        InternalServiceWrapperBuilder<O> swb = new InternalServiceWrapperBuilder<>();
        swb.addInputPath(inputPath);
        swb.outputPath = outputPath;
        swb.outClass = outClass;
        swb.addServiceFunction(serviceFunction);
        return swb;
    }

    @NotNull
    public List<String> inputPath() {
        return inputPaths;
    }

    @NotNull
    public InternalServiceWrapperBuilder<O> addInputPath(String inputPath) {
        this.inputPaths.add(inputPath);
        return this;
    }

    public Class<O> outClass() {
        return outClass;
    }

    @NotNull
    public InternalServiceWrapperBuilder<O> outClass(Class<O> outClass) {
        this.outClass = outClass;
        return this;
    }

    public String outputPath() {
        return outputPath;
    }

    @NotNull
    public InternalServiceWrapperBuilder<O> outputPath(String outputPath) {
        this.outputPath = outputPath;
        return this;
    }

    @NotNull
    public List<Function<O, Object>> getServiceFunctions() {
        return serviceFunctions;
    }

    @NotNull
    public InternalServiceWrapperBuilder<O> addServiceFunction(Function<O, Object> serviceFunctions) {
        this.serviceFunctions.add(serviceFunctions);
        return this;
    }

    public EventLoop eventLoop() {
        return eventLoop;
    }

    public boolean createdEventLoop() {
        return createdEventLoop;
    }

    public void eventLoop(EventLoop eventLoop) {
        this.eventLoop = eventLoop;
    }

    public HandlerPriority priority() {
        return priority;
    }

    @NotNull
    public InternalServiceWrapperBuilder<O> priority(HandlerPriority priority) {
        this.priority = priority;
        return this;
    }

    public int inputSourceId() {
        return inputSourceId;
    }

    @NotNull
    public InternalServiceWrapperBuilder<O> inputSourceId(int inputSourceId) {
        this.inputSourceId = inputSourceId;
        return this;
    }

    public int outputSourceId() {
        return outputSourceId;
    }

    @NotNull
    public InternalServiceWrapperBuilder<O> outputSourceId(int outputSourceId) {
        this.outputSourceId = outputSourceId;
        return this;
    }

    @NotNull
    @Override
    public ServiceWrapper get() {
        if (eventLoop == null) {
            eventLoop = new EventGroup(false);
            createdEventLoop = true;
        }
        return new InternalEventLoopServiceWrapper<>(this);
    }

    @NotNull
    public ChronicleQueue inputQueue() {
        SingleChronicleQueue build = SingleChronicleQueueBuilder
                .binary(inputPaths.get(0))
                .sourceId(inputSourceId())
                .checkInterrupts(false)
                .build();
        queues.add(build);
        return build;
    }

    @NotNull
    public ChronicleQueue outputQueue() {
        SingleChronicleQueue build = SingleChronicleQueueBuilder
                .binary(outputPath)
                .sourceId(outputSourceId())
                .checkInterrupts(false)
                .build();
        queues.add(build);
        return build;
    }

    @NotNull
    public MethodReader outputReader(Object... impls) {
        ChronicleQueue queue = outputQueue();
        MethodReader reader = queue.createTailer().methodReader(impls);
        reader.closeIn(true);
        return reader;
    }

    @NotNull
    public <T> T inputWriter(Class<T> tClass) {
        ChronicleQueue queue = inputQueue();
        return queue.acquireAppender()
                .methodWriterBuilder(tClass)
                .recordHistory(true)
                .onClose(queue)
                .get();
    }

    public void closeQueues() {
        Closeable.closeQuietly(queues);
    }
}
