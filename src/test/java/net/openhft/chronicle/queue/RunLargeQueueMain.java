/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.Jvm;

public enum RunLargeQueueMain {
    ; // none

    private static final int FILE_SIZE = Integer.getInteger("file.size", 1024);
    private static final int MSG_SIZE = Integer.getInteger("msg.size", 512);
    private static final double BLOCK_SIZE = Double.parseDouble(System.getProperty("block.size", "64"));
    private static final boolean PRETOUCH = Jvm.getBoolean("pretouch");

    public static void main(String[] args) {
        System.out.println("file.size: " + FILE_SIZE + " # GB");
        System.out.println("msg.size: " + MSG_SIZE + " # B");
        System.out.println("block.size: " + BLOCK_SIZE + " # MB");
        System.out.println("pretouch: " + PRETOUCH);
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(args[0]).blockSize((int) (BLOCK_SIZE * (1 << 20))).build()) {
            ExcerptAppender appender = queue.acquireAppender();
            ExcerptTailer tailer = queue.createTailer();
            BytesStore bytes = BytesStore.nativeStore(MSG_SIZE);
            Bytes<?> bytes2 = Bytes.allocateDirect(MSG_SIZE);
            for (int t = 1; t <= FILE_SIZE; t++) {
                long start = System.currentTimeMillis();
                for (int i = 0; i < 1 << 30; i += MSG_SIZE) {
                    appender.writeBytes(bytes);
                }
                long mid = System.currentTimeMillis();
                for (int i = 0; i < 1 << 30; i += MSG_SIZE) {
                    bytes2.clear();
                    tailer.readBytes(bytes2);
                }
                long end = System.currentTimeMillis();
                System.out.printf("%d: Took %.3f seconds to write and %.3f seconds to read 1 GB%n", t, (mid - start) / 1e3, (end - mid) / 1e3);
                if (PRETOUCH)
                    appender.pretouch();
                Jvm.pause(8_000);
            }
        }
    }
}
