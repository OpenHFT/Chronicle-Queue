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
package net.openhft.chronicle.tcp;

import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptCommon;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.tools.WrappedChronicle;
import net.openhft.chronicle.tools.WrappedExcerpt;

import java.io.IOException;

public class ChronicleSource extends WrappedChronicle {
    private final SourceTcp connection;
    private final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;
    private final Object notifier;

    private volatile boolean closed;

    public ChronicleSource(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, final SourceTcp connection) {
        super(builder.chronicle());
        this.builder = builder;
        this.notifier = new Object();
        this.closed = false;

        this.connection = connection;
        this.connection.notifier(this.notifier);
        this.connection.open();
    }

    @Override
    public void close() throws IOException {
        if(!closed) {
            closed = true;
            if (this.connection != null) {
                this.connection.close();
            }
        }

        super.close();
    }

    @Override
    public Excerpt createExcerpt() throws IOException {
        return new SourceExcerpt(wrappedChronicle.createExcerpt());
    }

    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new SourceExcerpt(wrappedChronicle.createTailer());
    }

    @Override
    public ExcerptAppender createAppender() throws IOException {
        return new SourceExcerpt(wrappedChronicle.createAppender());
    }

    /**
     * This excerpt notifies the replication engine that something has been done
     * on the underlying chronicle.
     */
    private final class SourceExcerpt extends WrappedExcerpt {
        public SourceExcerpt(final ExcerptCommon excerptCommon) {
            super(excerptCommon);
        }

        @Override
        public void finish() {
            super.finish();

            synchronized (notifier) {
                notifier.notifyAll();
            }
        }
    }
}
