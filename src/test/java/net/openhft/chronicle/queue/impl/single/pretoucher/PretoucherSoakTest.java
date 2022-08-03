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

package net.openhft.chronicle.queue.impl.single.pretoucher;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.MessageHistory;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.junit.Ignore;

import java.util.UUID;

@Ignore("stress test")
public class PretoucherSoakTest extends QueueTestCommon {

    public static void main(String[] args) {
        SingleChronicleQueue outQueue = SingleChronicleQueueBuilder.binary("target/" + "monitor")
                .rollCycle(RollCycles.TEST_SECONDLY).build();
        ExcerptAppender outQueueAppender = outQueue.acquireAppender();

        HeartbeatListener heartbeatWriter = outQueueAppender.methodWriterBuilder(HeartbeatListener.class).updateInterceptor((m, a) -> {
            ValidFields.validateAll(a);
            return true;
        }).get();

        long periodicUpdateUS = (long) 10 * 1000;

        long lastHB = 0;
        while (true) {
            if (System.currentTimeMillis() - lastHB > 1) {
                // write a hb to the queue
                MessageHistory.get().reset();
                Heartbeat heartBeat = new Heartbeat(UUID.randomUUID().toString());
                heartbeatWriter.heartbeat(heartBeat);
                lastHB = System.currentTimeMillis();
            }
        }
    }

    public interface HeartbeatListener {

        /**
         * called periodically under normal operation
         *
         */
        void heartbeat(Heartbeat heartbeat);

    }

    public static class Heartbeat extends SelfDescribingMarshallable implements Validatable {
        final String source;
        long time;

        public Heartbeat(String source) {
            this.source = source;
        }

        public String source() {
            return source;
        }

        public long time() {
            return time;
        }

        public Heartbeat time(long time) {
            this.time = time;
            return this;
        }

        @Override
        public void validate() throws IllegalStateException {

        }
    }
}