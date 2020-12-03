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
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.internal.service.InternalServiceWrapperBuilder;
import net.openhft.chronicle.threads.EventGroup;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public interface ServiceWrapper extends Closeable {

    ChronicleQueue[] inputQueues();

    ChronicleQueue outputQueue();

    static <O> Builder<O> builder(@NotNull final String inputPath,
                                  @NotNull final String outputPath,
                                  @NotNull final Class<O> outClass,
                                  @NotNull final Function<O, Object> serviceFunction) {
        return InternalServiceWrapperBuilder.serviceBuilder(inputPath, outputPath, outClass, serviceFunction);
    }


    interface Builder<O> extends Supplier<ServiceWrapper> {
        @NotNull
        List<String> inputPath();

        Class<O> outClass();

        @NotNull
        Builder<O> addInputPath(String inputPath);

        @NotNull
        Builder<O> outClass(Class<O> outClass);

        String outputPath();

        @NotNull
        Builder<O> outputPath(String outputPath);

        @NotNull
        List<Function<O, Object>> getServiceFunctions();

        @NotNull
        Builder<O> addServiceFunction(Function<O, Object> serviceFunctions);

        EventLoop eventLoop();

        boolean createdEventLoop();

        void eventLoop(EventLoop eventLoop);

        HandlerPriority priority();

        @NotNull
        Builder<O> priority(HandlerPriority priority);

        int inputSourceId();

        @NotNull
        Builder<O> inputSourceId(int inputSourceId);

        int outputSourceId();

        @NotNull
        Builder<O> outputSourceId(int outputSourceId);

        @NotNull
        @Override
        ServiceWrapper get();

        @NotNull
        ChronicleQueue inputQueue();

        @NotNull
        ChronicleQueue outputQueue();

        @NotNull
        MethodReader outputReader(Object... impls);

        @NotNull
        <T> T inputWriter(Class<T> tClass);

        void closeQueues();
    }

}
