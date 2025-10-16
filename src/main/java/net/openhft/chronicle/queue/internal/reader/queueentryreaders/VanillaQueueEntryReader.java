/*
 * Copyright 2016-2025 chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.internal.reader.queueentryreaders;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.reader.MessageConsumer;
import net.openhft.chronicle.queue.reader.QueueEntryHandler;
import net.openhft.chronicle.queue.reader.QueueEntryReader;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

public final class VanillaQueueEntryReader implements QueueEntryReader {

    private final ExcerptTailer tailer;
    private final Function<ExcerptTailer, DocumentContext> pollMethod;
    private final QueueEntryHandler messageConverter;
    private final MessageConsumer messageConsumer;

    public VanillaQueueEntryReader(@NotNull ExcerptTailer tailer, @NotNull Function<ExcerptTailer, DocumentContext> pollMethod,
                                   @NotNull QueueEntryHandler messageConverter, @NotNull MessageConsumer messageConsumer) {
        this.tailer = tailer;
        this.pollMethod = pollMethod;
        this.messageConverter = messageConverter;
        this.messageConsumer = messageConsumer;
    }

    @Override
    public boolean read() {
        try (DocumentContext dc = pollMethod.apply(tailer)) {
            if (!dc.isPresent()) {
                return false;
            }

            messageConverter.accept(dc.wire(), val -> messageConsumer.consume(dc.index(), val));
            return true;
        }
    }
}
