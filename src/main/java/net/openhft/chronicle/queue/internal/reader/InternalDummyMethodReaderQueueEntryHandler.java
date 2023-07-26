/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
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

package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.reader.QueueEntryHandler;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public final class InternalDummyMethodReaderQueueEntryHandler implements QueueEntryHandler {
    private final Bytes<?> textConversionTarget = Bytes.allocateElasticOnHeap();
    private final WireType wireType;

    public InternalDummyMethodReaderQueueEntryHandler(@NotNull WireType wireType) {
        this.wireType = requireNonNull(wireType);
    }

    @Override
    public void accept(final WireIn wireIn, final Consumer<String> messageHandler) {
        long elementCount = 0;
        while (wireIn.hasMore()) {
            new BinaryWire(wireIn.bytes()).copyOne(wireType.apply(textConversionTarget));

            elementCount++;
            if ((elementCount & 1) == 0) {
                messageHandler.accept(textConversionTarget.toString());
                textConversionTarget.clear();
            }
        }
    }

    @Override
    public void close() {
        textConversionTarget.releaseLast();
    }
}