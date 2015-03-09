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
package vanilla.java.tutorial;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;

import static net.openhft.lang.model.DataValueClasses.newDirectReference;

public class OffHeapDirectReference extends OffHeapHelper {

    public static void main(String[] ignored) throws Exception {
        final int items = 100;
        final String path = System.getProperty("java.io.tmpdir") + "/direct-instance";
        final Event event = newDirectReference(Event.class);

        try (Chronicle chronicle = ChronicleQueueBuilder.vanilla(path).build()) {
            chronicle.clear();

            ExcerptAppender appender = chronicle.createAppender();
            for(int i=0; i<items; i++) {
                appender.startExcerpt(event.maxSize());

                event.bytes(appender, 0);
                event.setOwner(0);
                event.setType(i / 10);
                event.setTimestamp(System.currentTimeMillis());
                event.setId(i);

                appender.position(event.maxSize());
                appender.finish();
            }

            appender.close();

            process(chronicle, items);
        }
    }
}
