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
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.file.Path;
import java.util.function.Consumer;

public interface Reader {

    void execute();

    boolean readOne(@NotNull QueueEntryHandler messageConverter,
                    @NotNull ExcerptTailer tailer,
                    @NotNull Consumer<String> messageConsumer);

    void stop();

    Reader withMessageSink(@NotNull Consumer<String> messageSink);

    Reader withBasePath(@NotNull Path path);

    Reader withInclusionRegex(@NotNull String regex);

    Reader withExclusionRegex(@NotNull String regex);

    Reader withCustomPlugin(@NotNull ChronicleReaderPlugin customPlugin);

    Reader withStartIndex(final long index);

    Reader tail();

    Reader historyRecords(final long maxHistoryRecords);

    Reader asMethodReader(@Nullable String methodReaderInterface);

    Reader withWireType(@NotNull WireType wireType);

    Reader suppressDisplayIndex();

    static Reader create() {
        return new ChronicleReader();
    }
}