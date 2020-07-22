/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.sql.DriverManager;
import java.util.concurrent.atomic.AtomicLong;

public class JDBCServiceTest extends ChronicleQueueTestBase {

    @Test
    public void testCreateTable() {
        threadDump.ignore("HSQLDB Timer ");
        doCreateTable(4, 5000);
    }

    @Test
    @Ignore("Long running")
    public void perfCreateTable() {
        doCreateTable(5, 200000);
    }

    private void doCreateTable(int repeats, int noUpdates) {
        for (int t = 0; t < repeats; t++) {
            long start = System.nanoTime(), written;
            File path1 = getTmpDir();
            File path2 = getTmpDir();
            File file = new File(OS.TARGET, "hsqldb-" + System.nanoTime());
            file.deleteOnExit();

            try (ChronicleQueue in = SingleChronicleQueueBuilder
                    .binary(path1)
                    .testBlockSize()
                    .build();
                 ChronicleQueue out = SingleChronicleQueueBuilder
                         .binary(path2)
                         .testBlockSize()
                         .build()) {

                try (JDBCService service = new JDBCService(in, out, () -> DriverManager.getConnection("jdbc:hsqldb:file:" + file.getAbsolutePath(), "SA", ""))) {

                    JDBCStatement writer = service.createWriter();
                    writer.executeUpdate("CREATE TABLE tableName (\n" +
                            "name VARCHAR(64) NOT NULL,\n" +
                            "num INT\n" +
                            ")\n");

                    for (int i = 1; i < (long) noUpdates; i++)
                        writer.executeUpdate("INSERT INTO tableName (name, num)\n" +
                                "VALUES (?, ?)", "name", i);

                    written = System.nanoTime() - start;
                    AtomicLong queries = new AtomicLong();
                    AtomicLong updates = new AtomicLong();
                    CountingJDBCResult countingJDBCResult = new CountingJDBCResult(queries, updates);
                    MethodReader methodReader = service.createReader(countingJDBCResult);
                    while (updates.get() < noUpdates) {
                        if (!methodReader.readOne())
                            Thread.yield();
                    }
                    Closeable.closeQuietly(service);

                    long time = System.nanoTime() - start;
                    System.out.printf("Average time to write each update %.1f us, average time to perform each update %.1f us%n",
                            written / noUpdates / 1e3,
                            time / noUpdates / 1e3);
                }
            } finally {
                try {
                    IOTools.deleteDirWithFiles(path1, 2);
                    IOTools.deleteDirWithFiles(path2, 2);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void afterChecks() {
        try {
            super.afterChecks();
        } catch (Throwable t) {
            // TODO FIX
            t.printStackTrace();
        }
    }
}
