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
package net.openhft.chronicle.tcp2;

import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptCommon;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.tools.WrappedChronicle;
import net.openhft.chronicle.tools.WrappedExcerpt;

import java.io.IOException;

public class ChronicleSource2 extends WrappedChronicle {
    private static final long BUSY_WAIT_TIME_NS = 100 * 1000;

    private final TcpConnection connection;
    private final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;
    private final Object notifier;
    private volatile boolean closed;
    private long lastUnpausedNS;
    private long pauseWait;

    public ChronicleSource2(final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, final TcpConnection connection) {
        super(builder.chronicle());
        this.connection = connection;
        this.builder = builder;
        this.notifier = new Object();
        this.closed = false;
        this.lastUnpausedNS = 0;
        this.pauseWait = builder.heartbeatIntervalUnit().toMillis(builder.heartbeatInterval()) / 2;
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

    // *************************************************************************
    //
    // *************************************************************************

    private void pauseReset() {
        lastUnpausedNS = System.nanoTime();
    }

    private void pause() {
        if (lastUnpausedNS + BUSY_WAIT_TIME_NS > System.nanoTime()) {
            return;
        }

        try {
            synchronized (notifier) {
                notifier.wait(pauseWait);
            }
        } catch (InterruptedException ie) {
            //logger.warn("Interrupt ignored");
        }
    }

    private void wakeSessionHandlers() {
        synchronized (notifier) {
            notifier.notifyAll();
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    private final class SourceExcerpt extends WrappedExcerpt {
        public SourceExcerpt(final ExcerptCommon excerptCommon) {
            super(excerptCommon);
        }

        @Override
        public void finish() {
            super.finish();
            wakeSessionHandlers();
        }
    }
}
