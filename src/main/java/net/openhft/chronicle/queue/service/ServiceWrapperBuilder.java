/*
 * Copyright 2016 higherfrequencytrading.com
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
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.EventGroup;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;


public class ServiceWrapperBuilder<O> implements Supplier<ServiceWrapper> {
    private final List<String> inputPaths = new ArrayList<>();
    private final List<Function<O, Object>> serviceFunctions = new ArrayList<>();
    private String outputPath;
    private Class<O> outClass;
    private EventLoop eventLoop;
    private HandlerPriority priority = HandlerPriority.MEDIUM;
    private boolean createdEventLoop = false;
    private int inputSourceId;
    private int outputSourceId;

    ServiceWrapperBuilder() {
    }

    @NotNull
    public static <O> ServiceWrapperBuilder<O> serviceBuilder(String inputPath, String outputPath, Class<O> outClass, Function<O, Object> serviceFunction) {
        ServiceWrapperBuilder<O> swb = new ServiceWrapperBuilder<>();
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
    public ServiceWrapperBuilder<O> addInputPath(String inputPath) {
        this.inputPaths.add(inputPath);
        return this;
    }

    public Class<O> outClass() {
        return outClass;
    }

    @NotNull
    public ServiceWrapperBuilder<O> outClass(Class<O> outClass) {
        this.outClass = outClass;
        return this;
    }

    public String outputPath() {
        return outputPath;
    }

    @NotNull
    public ServiceWrapperBuilder<O> outputPath(String outputPath) {
        this.outputPath = outputPath;
        return this;
    }

    @NotNull
    public List<Function<O, Object>> getServiceFunctions() {
        return serviceFunctions;
    }

    @NotNull
    public ServiceWrapperBuilder<O> addServiceFunction(Function<O, Object> serviceFunctions) {
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
    public ServiceWrapperBuilder<O> priority(HandlerPriority priority) {
        this.priority = priority;
        return this;
    }

    public int inputSourceId() {
        return inputSourceId;
    }

    @NotNull
    public ServiceWrapperBuilder<O> inputSourceId(int inputSourceId) {
        this.inputSourceId = inputSourceId;
        return this;
    }

    public int outputSourceId() {
        return outputSourceId;
    }

    @NotNull
    public ServiceWrapperBuilder<O> outputSourceId(int outputSourceId) {
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
        return new EventLoopServiceWrapper<>(this);
    }

    @NotNull
    public ChronicleQueue inputQueue() {
        return SingleChronicleQueueBuilder
                .binary(inputPaths.get(0))
                .sourceId(inputSourceId())
                .checkInterrupts(false)
                .build();
    }

    @NotNull
    public ChronicleQueue outputQueue() {
        return SingleChronicleQueueBuilder
                .binary(outputPath)
                .sourceId(outputSourceId())
                .checkInterrupts(false)
                .build();
    }

    @NotNull
    public MethodReader outputReader(Object... impls) {
        ChronicleQueue queue = outputQueue();
        MethodReader reader = queue.createTailer().methodReader(impls);
        reader.closeIn(true);
        return reader;
    }

    @SuppressWarnings("deprecation")
    @NotNull
    public <T> T inputWriter(Class<T> tClass) {
        ChronicleQueue queue = inputQueue();
        return queue.acquireAppender()
                .methodWriterBuilder(tClass)
                .recordHistory(true)
                .onClose(queue)
                .get();
    }
}
