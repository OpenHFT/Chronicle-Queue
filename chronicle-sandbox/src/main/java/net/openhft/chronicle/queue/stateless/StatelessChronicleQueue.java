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

package net.openhft.chronicle.queue.stateless;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.Excerpt;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.stateless.bytes.StatelessRawBytesAppender;
import net.openhft.chronicle.queue.stateless.bytes.StatelessRawBytesTailer;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.function.Function;

/**
 * todo : currently work in process
 * <p>
 * Created by Rob Austin
 */
public class StatelessChronicleQueue implements ChronicleQueue {

    private final Function<Bytes<?>, Wire> wireFunction;
    private final StatelessRawBytesTailer statelessRawBytesTailer;
    private final StatelessRawBytesAppender statelessRawBytesAppender;

    public StatelessChronicleQueue(Function<Bytes<?>, Wire> wireFunction,
                                   StatelessRawBytesTailer statelessRawBytesTailer,
                                   StatelessRawBytesAppender statelessRawBytesAppender) {
        this.wireFunction = wireFunction;
        this.statelessRawBytesTailer = statelessRawBytesTailer;
        this.statelessRawBytesAppender = statelessRawBytesAppender;
    }

    @NotNull
    @Override
    public String name() {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public Excerpt createExcerpt() throws IOException {
        return new StatelessExcerpt(this, wireFunction, statelessRawBytesTailer);
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new StatelessTailer(this, wireFunction, statelessRawBytesTailer);
    }

    @NotNull
    @Override
    public ExcerptAppender createAppender() throws IOException {
        return new StatelessAppender(this, statelessRawBytesAppender);
    }

    @Override
    public long size() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public long firstAvailableIndex() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public long lastWrittenIndex() {
        return statelessRawBytesTailer.lastWrittenIndex();
    }

    @Override
    public void close() {
        // todo drop the socket connection
    }
}
