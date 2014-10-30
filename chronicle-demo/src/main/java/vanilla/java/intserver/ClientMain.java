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

package vanilla.java.intserver;

import net.openhft.chronicle.*;
import net.openhft.chronicle.tools.ChronicleTools;
import vanilla.java.intserver.api.C2SWriter;
import vanilla.java.intserver.api.IClient;
import vanilla.java.intserver.api.S2CReader;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientMain {
    public static void main(String... ignored) throws IOException {
        long start = System.currentTimeMillis();
        String tmp = System.getProperty("java.io.tmpdir");

        String c2sPath = tmp + "/demo/c2s";
        ChronicleTools.deleteDirOnExit(c2sPath);
        Chronicle c2s = ChronicleQueueBuilder.indexed(c2sPath).build();
        ExcerptAppender appender = c2s.createAppender();
        C2SWriter c2sWriter = new C2SWriter(appender);

        String s2cPath = tmp + "/demo/s2c";
        Chronicle s2c = ChronicleQueueBuilder.indexed(s2cPath).build();
        ExcerptTailer tailer = s2c.createTailer();
        final AtomicInteger received = new AtomicInteger();
        S2CReader s2cReader = new S2CReader(new IClient() {
            @Override
            public void response(int request, int response, Object... args) {
                received.incrementAndGet();
            }
        });

        int runs = 5000000;
        for (int i = 0; i < runs; i++) {
            c2sWriter.command(i);
            // catch up if any pending messages.
            while (s2cReader.readOne(tailer)) ;
        }

        while (received.get() < runs) {
            while (s2cReader.readOne(tailer)) ;
            System.out.println(received.get());
        }
        long time = System.currentTimeMillis() - start;
        System.out.printf("Took %.1f seconds for %,d messages%n", time / 1e3, runs);
    }
}
