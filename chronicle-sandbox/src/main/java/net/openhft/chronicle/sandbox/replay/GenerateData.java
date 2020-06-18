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

package net.openhft.chronicle.sandbox.replay;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.lang.model.DataValueClasses;

import java.io.IOException;

public class GenerateData {
    /*
    On an i7-3970X prints.

    Took 1.383 seconds to write 10,000,000 records
     */
    static final long RECORDS = Long.getLong("RECORDS", 10000000);

    public static void main(String[] args) throws IOException {
        TestData td = DataValueClasses.newInstance(TestData.class);
        String path = "/tmp/test";
        StringBuilder name = new StringBuilder();

        long start = System.nanoTime();
        try (Chronicle chronicle = ChronicleQueueBuilder.indexed(path).build()) {
            ExcerptAppender appender = chronicle.createAppender();
            for (long i = 0; i < RECORDS; i++) {
                name.setLength(0);
                td.setName(name.append("Name").append(i));
                td.setAge(i);
                td.setImportance((double) i / RECORDS);
                td.setTimestamp(System.currentTimeMillis());

                appender.startExcerpt();
                td.writeMarshallable(appender);
                appender.finish();
            }
        }
        System.out.printf("Took %.3f seconds to write %,d records%n",
                (System.nanoTime() - start) / 1e9, RECORDS);
    }
}
