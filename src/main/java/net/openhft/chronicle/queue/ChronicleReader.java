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

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;

import java.io.File;
import java.io.IOException;

/**
 * Display records in a Chronicle in a text form.
 *
 * @author peter.lawrey
 */
public enum ChronicleReader {
    ;

    public static void main(String... args) throws IOException, InterruptedException {
        if (args.length < 1) {
            System.err.println("Usage: java " + ChronicleReader.class.getName() + " {chronicle-base-path} [from-index]");
            System.exit(-1);
        }

        String basePath = args[0];
        ChronicleQueue ic = SingleChronicleQueueBuilder.binary(new File(basePath)).build();
        ExcerptTailer tailer = ic.createTailer();
        if (args.length > 1) {
            long index = Long.parseLong(args[1]);
            while (!tailer.moveToIndex(index))
                Thread.sleep(50);
        }

        //noinspection InfiniteLoopStatement
        while (true) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent()) {
                    Thread.sleep(50);
                    continue;
                }
                System.out.print(Long.toHexString(dc.index()) + ": ");
                Bytes<?> bytes = dc.wire().bytes();
                byte b0 = bytes.readByte(0);
                if (b0 < 0) {
                    System.out.println(bytes.toHexString());
                } else {
                    System.out.println(bytes.toString());
                }
            }
            System.out.println();
        }
    }
}
