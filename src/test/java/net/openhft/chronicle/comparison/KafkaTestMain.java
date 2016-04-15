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

package net.openhft.chronicle.comparison;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.tools.ChronicleTools;

import java.io.IOException;

/**
 * Based on this test for http://kafka.apache.org/07/performance.html
 * The aim appears to be see the message rate in MB/s the software can drive.
 *
 * @author peter.lawrey
 */
public class KafkaTestMain {
    static final int message_size = Integer.getInteger("message_size", 200); // bytes
    static final int batch_size = Integer.getInteger("batch_size", 200); // messages
    static final int fetch_size = 1 * 1024 * 1024; // bytes
    static final int flush_interval = Integer.getInteger("flush_count", Integer.MAX_VALUE); // messages
    static final int flush_period = Integer.getInteger("flush_period", Integer.MAX_VALUE); // milli-seconds

    public static void main(String... ignored) throws IOException {
        System.out.print("message_size=" + message_size);
        System.out.print(", batch_size=" + batch_size);
        System.out.print(", flush_interval=" + flush_interval);
        System.out.print(", flush_period=" + flush_period);
        System.out.println();
        ChronicleTools.warmup();
        String basePath = System.getProperty("java.io.tmpdir") + "/kafka-test";
        ChronicleTools.deleteDirOnExit(basePath);
        long start = System.nanoTime();
        long lastUpdate = System.currentTimeMillis();
        Chronicle chronicle = ChronicleQueueBuilder.vanilla(basePath).build();
        byte[] bytes = new byte[message_size];
        ExcerptAppender e = chronicle.createAppender();
        int count = 50 * 1000 * 1000;
        for (int i = 0; i < count; i += batch_size) {
            // tune for very large messages.
            e.startExcerpt(batch_size * (message_size + 4));
            for (int j = 0; j < batch_size; j++) {
                e.writeInt(bytes.length);
                e.write(bytes);
            }
            e.flush();
            if (flush_interval < Integer.MAX_VALUE)
                e.nextSynchronous(i % flush_interval == 0);
            else if (lastUpdate + flush_period <= System.currentTimeMillis()) {
                e.nextSynchronous(true);
                lastUpdate += flush_period; // don't use the current time in case we are slow.
            }
            e.finish();
        }
        chronicle.close();
        long time = System.nanoTime() - start;
        double MB = (double) count * message_size / 1e6;
        double secs = time / 1e9;
        System.out.printf("Throughput of %.1f MB/sec%n", MB / secs);
    }
}
