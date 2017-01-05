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

package net.openhft.chronicle.examples;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestIndex {
    private static final Logger LOG = LoggerFactory.getLogger(TestIndex.class);

    private static volatile boolean running = true;

    public static void main(String[] args) throws Exception {
        final Chronicle chronicle1 = ChronicleQueueBuilder.vanilla("./test1")
                // use small blocks to increase the chances of a bug showing.
                .dataBlockSize(4 << 10) // 4K
                .indexBlockSize(4 << 10) // 4K
                .build();

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    final ExcerptAppender appender = chronicle1.createAppender();
                    while (running) {
                        appender.startExcerpt(1024);
                        appender.writeLong(System.currentTimeMillis());
                        appender.finish();
                    }
                    appender.close();
                } catch (Exception e) {
                    LOG.error("Error", e);
                    e.printStackTrace();
                }
            }
        });
        t1.start();

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    long lastIndex = 0;
                    while (running) {
                        lastIndex = chronicle1.lastIndex();
                        LOG.trace("last index = {}", lastIndex);
                        Thread.sleep(10);
                    }
                } catch (Exception e) {
                    LOG.error("Error", e);
                    e.printStackTrace();
                }
            }
        });
        t2.start();

        Thread.sleep(5000);

        running = false;

        t1.join();
        t2.join();

//        chronicle1.close();
    }
}