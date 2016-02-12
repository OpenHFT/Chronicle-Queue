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
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.TextWire;

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
            System.err.println("Usage: java " + ChronicleReader.class.getName() + " {chronicle-base-path} {regex} [from-index]");
            System.exit(-1);
        }

        String basePath = args[0];
        String regex = args.length > 1 ? args[1] : "";
        long index = args.length > 2 ? Long.decode(args[2]) : 0;
        tailFileFrom(basePath, regex, index, false);
    }

    public static void tailFileFrom(String basePath, String regex, long index, boolean stopAtEnd) throws IOException, InterruptedException {
        ChronicleQueue ic = SingleChronicleQueueBuilder.binary(new File(basePath)).build();
        ExcerptTailer tailer = ic.createTailer();
        if (index > 0) {
            System.out.println("Waiting for index " + index);
            while (!tailer.moveToIndex(index)) {
                Thread.sleep(500);
            }
        }

        Bytes bytes2 = Bytes.elasticByteBuffer();
        //noinspection InfiniteLoopStatement
        while (true) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent()) {
                    if (stopAtEnd)
                        break;
                    Thread.sleep(50);
                    continue;
                }
                Bytes<?> bytes = dc.wire().bytes();
                byte b0 = bytes.readByte(bytes.readPosition());
                String text;
                if (b0 < 0) {
                    bytes2.clear();
                    new BinaryWire(bytes).copyTo(new TextWire(bytes2));
                    text = bytes2.toString();
                } else {
                    text = bytes.toString();
                }
                if (regex.isEmpty() || text.matches(regex)) {
                    System.out.print("0x" + Long.toHexString(dc.index()) + ": ");
                    System.out.println(text);
                }
            }
        }
    }
}
