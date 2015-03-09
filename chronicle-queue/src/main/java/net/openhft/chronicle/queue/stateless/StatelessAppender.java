/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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

package net.openhft.chronicle.queue.stateless;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.stateless.bytes.StatelessRawBytesAppender;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

/**
 * todo : currently work in process
 *
 * Created by Rob Austin
 */
public class StatelessAppender implements ExcerptAppender {

    @NotNull
    private final ChronicleQueue chronicleQueue;
    @NotNull
    private final StatelessRawBytesAppender statelessRawBytesAppender;
    private long lastWrittenIndex = -1;

    public StatelessAppender(@NotNull ChronicleQueue chronicleQueue,
                             @NotNull StatelessRawBytesAppender statelessRawBytesAppender) {
        this.chronicleQueue = chronicleQueue;
        this.statelessRawBytesAppender = statelessRawBytesAppender;
    }

    @NotNull
    @Override
    public WireOut wire() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeDocument(@NotNull Consumer<WireOut> writer) {
        WireOut wire = wire();
        writer.accept(wire);
        lastWrittenIndex = statelessRawBytesAppender.appendExcept(wire.bytes());
    }

    @Override
    public long lastWrittenIndex() {
        return lastWrittenIndex;
    }

    @NotNull
    @Override
    public ChronicleQueue chronicle() {
        return chronicleQueue;
    }
}
