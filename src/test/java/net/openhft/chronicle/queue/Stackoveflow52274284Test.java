/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
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

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Stackoveflow52274284Test extends QueueTestCommon {
    @Test
    public void fails() throws IOException {
        String basePath = OS.getTarget();
        String path = Files.createTempDirectory(Paths.get(basePath), "chronicle-")
                .toAbsolutePath()
                .toString();
        // System.out.printf("Using temp path '%s'%n", path);

        try (ChronicleQueue chronicleQueue = ChronicleQueue.singleBuilder(path).testBlockSize().build();

             // Create Appender
             ExcerptAppender appender = chronicleQueue.createAppender();

             // Create Tailer
             ExcerptTailer tailer = chronicleQueue.createTailer()) {
            tailer.toStart();

            int numberOfRecords = 10;

            // Write
            for (int i = 0; i <= numberOfRecords; i++) {
                // System.out.println("Writing " + i);
                try (final DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("msg").text("Hello World!");
                    // System.out.println("your data was store to index=" + dc.index());
                } catch (Exception e) {
                    System.err.println("Unable to store value to chronicle");
                    e.printStackTrace();
                }
            }
            // Read
            for (int i = 0; i <= numberOfRecords; i++) {
                // System.out.println("Reading " + i);
                try (DocumentContext documentContext = tailer.readingDocument()) {
                    long currentOffset = documentContext.index();
                    // System.out.println("Current offset: " + currentOffset);

                    Wire wire = documentContext.wire();

                    if (wire != null) {
                        String msg = wire
                                .read("msg")
                                .text();
                        // System.out.println(msg);
                    }
                }
            }
        }
        IOTools.deleteDirWithFiles(path);
    }
}
