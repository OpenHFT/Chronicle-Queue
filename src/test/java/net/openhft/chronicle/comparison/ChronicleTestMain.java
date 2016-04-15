/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.comparison;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.tools.ChronicleTools;

import java.io.IOException;

/**
 * Based on this test for http://kafka.apache.org/07/performance.html
 * The aim appears to be see the message rate in MB/s the software can drive.
 *
 * @author peter.lawrey
 */
public class ChronicleTestMain {
    static final int message_size = Integer.getInteger("message_size", 100); // bytes
    static final int batch_size = Integer.getInteger("batch_size", 200); // messages
    static final int message_number = Integer.getInteger("message_number", 1000000); // messages number
    static final int fetch_size = 1 * 1024 * 1024; // bytes
    static final int flush_interval = Integer.getInteger("flush_count", Integer.MAX_VALUE); // messages
    static final int flush_period = Integer.getInteger("flush_period", 100); // milli-seconds

    public static void main(String... ignored) throws IOException {
        System.out.println("message_size, message_number, batch_size, processing_time, flush_count, flush_period");

        ChronicleTools.warmup();
        String basePath = System.getProperty("java.io.tmpdir") + "/test";
        ChronicleTools.deleteDirOnExit(basePath);

        long start = System.currentTimeMillis();
        long lastUpdate = System.currentTimeMillis();

        ChronicleTools.deleteOnExit(basePath);

        Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath).build();
        byte[] bytes = new byte[message_size];
        ExcerptAppender e = chronicle.createAppender();

        for (int i = 0; i < message_number; i += batch_size) {
            // tune for very large messages.
            e.startExcerpt(batch_size * (message_size + 4));
            for (int j = 0; j < batch_size; j++) {
                e.writeInt(bytes.length);
                e.write(bytes);
            }
            e.flush();
            if (flush_interval < Integer.MAX_VALUE) {
                e.nextSynchronous(i % flush_interval == 0);
            } else if (lastUpdate + flush_period <= System.currentTimeMillis()) {
                e.nextSynchronous(true);
                lastUpdate += flush_period; // don't use the current time in case we are slow.
            }
            e.finish();
        }
        chronicle.close();
        long time = System.currentTimeMillis() - start;
        System.out.println(message_size + ", " + message_number + ", " + batch_size + ", " + time + ", " + flush_interval + ", " + flush_period);

    }
}