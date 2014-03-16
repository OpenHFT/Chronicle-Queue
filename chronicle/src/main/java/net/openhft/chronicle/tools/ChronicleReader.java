/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.tools;

import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.lang.model.constraints.NotNull;

import java.io.IOException;

/**
 * Display records in a Chronicle in a text form.
 *
 * @author peter.lawrey
 */
enum ChronicleReader {
    ;

    public static void main(@NotNull String... args) throws IOException, InterruptedException {
        if (args.length < 1) {
            System.err.println("Usage: java " + ChronicleReader.class.getName() + " {chronicle-base-path} [from-index]");
            System.exit(-1);
        }

        String basePath = args[0];
        long index = args.length > 1 ? Long.parseLong(args[1]) : 0L;
        IndexedChronicle ic = new IndexedChronicle(basePath);
        ExcerptTailer excerpt = ic.createTailer();
        //noinspection InfiniteLoopStatement
        while (true) {
            while (!excerpt.nextIndex())
                //noinspection BusyWait
                Thread.sleep(50);
            System.out.print(index + ": ");
            int nullCount = 0;
            while (excerpt.remaining() > 0) {
                char ch = (char) excerpt.readUnsignedByte();
                if (ch == 0) {
                    nullCount++;
                    continue;
                }
                if (nullCount > 0)
                    System.out.print(" " + nullCount + "*\\0");
                nullCount = 0;
                if (ch < ' ')
                    System.out.print("^" + (char) (ch + '@'));
                else if (ch > 126)
                    System.out.print("\\x" + Integer.toHexString(ch));
                else
                    System.out.print(ch);
            }
            if (nullCount > 0)
                System.out.print(" " + nullCount + "*\\0");
            System.out.println();
            index++;
        }
    }
}
