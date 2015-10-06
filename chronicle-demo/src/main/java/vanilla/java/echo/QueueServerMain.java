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

package vanilla.java.echo;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;

import java.io.IOException;

public class QueueServerMain {
    public static void main(String... args) throws IOException {
        String host = args[0];

        Chronicle inbound = ChronicleQueueBuilder
                .indexed("/tmp/server-inbound")
                .sink().connectAddress(host, 54001)
                .build();
        ExcerptTailer tailer = inbound.createTailer().toEnd();

        Chronicle outbound = ChronicleQueueBuilder
                .indexed("/tmp/server-outbound")
                .source().bindAddress(54002)
                .build();

        ExcerptAppender appender = outbound.createAppender();

        if (!host.equals("localhost"))
            AffinityLock.acquireLock();

        long count = 0, next = 1000000;
        while (true) {
            if (tailer.nextIndex()) {
                appender.startExcerpt();
                appender.write(tailer);
                appender.finish();
                count++;
/*
                System.out.print(".");
                if ((count & 127) == 0)
                    System.out.println();
*/
            } else {
                if (count >= next)
                    System.out.println(count);
                next += 1000000;
            }
        }
    }
}
