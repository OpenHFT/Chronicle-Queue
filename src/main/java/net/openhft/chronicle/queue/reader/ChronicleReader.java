/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.internal.reader.InternalChronicleReader;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.util.function.Consumer;

@Deprecated /* For removal in x.22., use Reader.create() instead */
public final class ChronicleReader implements Reader {

    private final InternalChronicleReader delegate = new InternalChronicleReader();

    @Override
    public void execute() {
        delegate.execute();
    }

    @Override
    public boolean readOne(@NotNull final QueueEntryHandler messageConverter,
                           @NotNull final ExcerptTailer tailer,
                           @NotNull final Consumer<String> messageConsumer) {

        return delegate.readOne(messageConverter, tailer, messageConsumer);
    }

    @Override
    public ChronicleReader withMessageSink(@NotNull Consumer<? super String> messageSink) {
        delegate.withMessageSink(messageSink);
        return this;
    }

    public Consumer<? super String> messageSink() {
        return delegate.messageSink();
    }

    @Override
    public ChronicleReader withBasePath(final @NotNull Path path) {
        delegate.withBasePath(path);
        return this;
    }

    @Override
    public ChronicleReader withInclusionRegex(final @NotNull String regex) {
        delegate.withInclusionRegex(regex);
        return this;
    }

    @Override
    public ChronicleReader withExclusionRegex(final @NotNull String regex) {
        delegate.withExclusionRegex(regex);
        return this;
    }

    @Override
    public ChronicleReader withCustomPlugin(final @NotNull ChronicleReaderPlugin customPlugin) {
        delegate.withCustomPlugin(customPlugin);
        return this;
    }

    @Override
    public ChronicleReader withStartIndex(final long index) {
        delegate.withStartIndex(index);
        return this;
    }

    @Override
    public ChronicleReader tail() {
        delegate.tail();
        return this;
    }

    @Override
    public ChronicleReader historyRecords(final long maxHistoryRecords) {
        delegate.historyRecords(maxHistoryRecords);
        return this;
    }

    @Override
    public ChronicleReader asMethodReader(String methodReaderInterface) {
        delegate.asMethodReader(methodReaderInterface);
        return this;
    }

    @Override
    public ChronicleReader withWireType(@NotNull WireType wireType) {
        delegate.withWireType(wireType);
        return this;
    }

    @Override
    public ChronicleReader suppressDisplayIndex() {
        delegate.suppressDisplayIndex();
        return this;
    }

    /*
    // visible for testing only
    ChronicleReader withDocumentPollMethod(final Function<ExcerptTailer, DocumentContext> pollMethod) {
        delegate.withDocumentPollMethod(pollMethod);
        return this;
    }
    */

    public void stop() {
        delegate.stop();
    }
}