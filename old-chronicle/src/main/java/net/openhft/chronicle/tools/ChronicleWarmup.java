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
package net.openhft.chronicle.tools;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;

import java.io.IOException;

public final class ChronicleWarmup {

    public static final class Indexed {

        public static final boolean DONE;
        private static final int WARMUP_ITER = 200000;
        private static final String TMP = System.getProperty("java.io.tmpdir");

        static {
            String basePath = TMP + "/warmup-" + Math.random();
            ChronicleTools.deleteOnExit(basePath);
            try {
                Chronicle ic = ChronicleQueueBuilder.indexed(basePath)
                    .dataBlockSize(64)
                    .indexBlockSize(64)
                    .build();

                ExcerptAppender appender = ic.createAppender();
                ExcerptTailer tailer = ic.createTailer();
                for (int i = 0; i < WARMUP_ITER; i++) {
                    appender.startExcerpt();
                    appender.writeInt(i);
                    appender.finish();
                    boolean b = tailer.nextIndex() || tailer.nextIndex();
                    tailer.readInt();
                    tailer.finish();
                }
                ic.close();
                System.gc();
                DONE = true;
            } catch (IOException e) {
                throw new AssertionError();
            }
        }
    }
}
