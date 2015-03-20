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
import net.openhft.chronicle.ExcerptAppender;
import org.junit.Test;

import static net.openhft.chronicle.ChronicleQueueBuilder.indexed;
import static net.openhft.chronicle.ChronicleQueueBuilder.remoteAppender;

public class StatelessIndexedChronicleAppenderTest extends StatelessChronicleTestBase {

    @Test(expected = IllegalStateException.class)
    public void testIndexedStatelessAppenderReject() throws Exception {
        final String basePathSource = getIndexedTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = indexed(basePathSource)
            .source()
            .bindAddress(0)
            .connectionListener(portSupplier)
            .build();

        final Chronicle remoteAppender = remoteAppender()
            .connectAddress("localhost", portSupplier.getAndAssertOnError())
            .appendRequireAck(true)
            .build();

        final ExcerptAppender appender = remoteAppender.createAppender();

        try {
            source.clear();

            appender.startExcerpt(16);
            appender.writeLong(1);
            appender.writeLong(2);
            appender.finish();
            appender.close();
        } finally {
            source.close();
            source.clear();
        }
    }

    @Test
    public void testIndexedStatelessAppenderDiscard() throws Exception {
        final String basePathSource = getIndexedTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = indexed(basePathSource)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        final Chronicle remoteAppender = remoteAppender()
                .connectAddress("localhost", portSupplier.getAndAssertOnError())
                .appendRequireAck(false)
                .build();

        final ExcerptAppender appender = remoteAppender.createAppender();

        try {
            source.clear();

            appender.startExcerpt(16);
            appender.writeLong(1);
            appender.writeLong(2);
            appender.finish();
            appender.close();
        } finally {
            source.close();
            source.clear();
        }
    }
}
