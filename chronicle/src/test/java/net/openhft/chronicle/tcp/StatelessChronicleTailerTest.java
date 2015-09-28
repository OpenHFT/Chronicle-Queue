/*
 * Copyright 2015 Higher Frequency Trading
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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;

public class StatelessChronicleTailerTest extends StatelessChronicleTestBase {

    @Test
    public void testRemoteTailerReconnect() throws IOException {
        final int attempts = 10;
        long start = System.currentTimeMillis();
        try (Chronicle chronicle = ChronicleQueueBuilder
                .remoteTailer()
                .connectAddress("localhost", 1)
                .build()) {
            ExcerptTailer tailer = chronicle.createTailer();
            for (int i = 0; i < attempts; i++) {
                assertFalse(tailer.nextIndex());
            }
        }

        System.out.printf("Took %,d ms for %d failed attempts%n", System.currentTimeMillis() - start, attempts);
    }

    @Test
    public void testRemoteTailerReconnectWithSpin() throws IOException {
        final long start = System.currentTimeMillis();
        final int attempts = 10;
        try (Chronicle chronicle = ChronicleQueueBuilder
                .remoteTailer()
                .connectAddress("localhost", 1)
                .reconnectionInterval(250, TimeUnit.MILLISECONDS)
                .reconnectionAttempts(4)
                .reconnectionWarningThreshold(3)
                .build()) {
            ExcerptTailer tailer = chronicle.createTailer();
            for (int i = 0; i < attempts; i++) {
                assertFalse(tailer.nextIndex());
            }
        }
        System.out.printf("Took %,d ms for %d failed attempts%n", System.currentTimeMillis() - start, attempts);
    }
}
