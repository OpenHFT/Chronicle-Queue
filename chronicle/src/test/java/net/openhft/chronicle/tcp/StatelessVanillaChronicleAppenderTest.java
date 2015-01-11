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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StatelessVanillaChronicleAppenderTest extends StatelessChronicleTestBase {

    @Ignore("not yet ready")
    @Test
    public void testVanillaStatelessAppender_001() throws Exception {
        final String basePathSource = getVanillaTestPath("-source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.vanilla(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        final int port = portSupplier.getAndCheckPort();
        final Chronicle remoteAppender = ChronicleQueueBuilder.remoteAppender()
            .connectAddress("localhost", port)
            .build();
        final Chronicle remoteTailer = ChronicleQueueBuilder.remoteTailer()
            .connectAddress("localhost", port)
            .build();

        final int items = 100;
        final ExcerptAppender appender = remoteAppender.createAppender();
        final ExcerptTailer tailer = remoteTailer.createTailer();

        try {
            for (long i = 1; i <= items; i++) {
                appender.startExcerpt(16);
                appender.writeLong(i);
                appender.writeLong(i);
                appender.finish();
            }

            appender.close();

            /*
            for (long i = 1; i <= items; i++) {
                assertTrue(tailer.nextIndex());
                assertEquals(i, tailer.readLong());
                tailer.finish();
            }

            tailer.close();
            */
        } finally {
            source.close();
            source.clear();

            assertFalse(new File(basePathSource).exists());
        }
    }
}
