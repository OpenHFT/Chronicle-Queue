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

package vanilla.java.single;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;

import java.io.File;
import java.io.IOException;
import java.util.Date;

public class QueueMain {
    public static void main(String... ignored) throws IOException {
        Chronicle chronicle = ChronicleQueueBuilder.vanilla(new File("my-queue8")).build();

        ExcerptAppender appender = chronicle.createAppender();
        appender.startExcerpt();
        appender.writeLong(System.currentTimeMillis());
        appender.writeUTFΔ("Hello World");
        appender.finish();

        ExcerptTailer tailer = chronicle.createTailer().toStart();
        StringBuilder msg = new StringBuilder();
        while (tailer.nextIndex()) {
            long time = tailer.readLong();
            tailer.readUTFΔ(msg);
            tailer.finish();

            System.out.println(new Date(time) + " - " + msg);

            System.out.println(new Date(time) + " - " + msg);
        }
        chronicle.close();
    }
}
