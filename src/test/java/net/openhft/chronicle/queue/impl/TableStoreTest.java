package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TableStoreTest extends QueueTestCommon {
    @Test
    public void acquireValueFor() throws IOException {

        final File file = tempDir("table");
        file.mkdir();

        final File tempFile = Files.createTempFile((Path) file.toPath(), "table", SingleTableStore.SUFFIX).toFile();

        try (TableStore table = SingleTableBuilder.binary(tempFile, Metadata.NoMeta.INSTANCE).build();
             LongValue a = table.acquireValueFor("a");
             LongValue b = table.acquireValueFor("b")) {
            assertEquals(Long.MIN_VALUE, a.getVolatileValue());
            assertTrue(a.compareAndSwapValue(Long.MIN_VALUE, 1));
            assertEquals(Long.MIN_VALUE, b.getVolatileValue());
            assertTrue(b.compareAndSwapValue(Long.MIN_VALUE, 2));
            assertEquals("--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT\n" +
                    "}\n" +
                    "# position: 60, header: 0\n" +
                    "--- !!data #binary\n" +
                    "a: 1\n" +
                    "# position: 80, header: 1\n" +
                    "--- !!data #binary\n" +
                    "b: 2\n" +
                    "...\n" +
                    "# 130972 bytes remaining\n", table.dump());
        }

        try (TableStore table = SingleTableBuilder.binary(tempFile, Metadata.NoMeta.INSTANCE).build();
             LongValue c = table.acquireValueFor("c");
             LongValue b = table.acquireValueFor("b")) {
            assertEquals(Long.MIN_VALUE, c.getVolatileValue());
            assertTrue(c.compareAndSwapValue(Long.MIN_VALUE, 3));
            assertEquals(2, b.getVolatileValue());
            assertTrue(b.compareAndSwapValue(2, 22));
            assertEquals("--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT\n" +
                    "}\n" +
                    "# position: 60, header: 0\n" +
                    "--- !!data #binary\n" +
                    "a: 1\n" +
                    "# position: 80, header: 1\n" +
                    "--- !!data #binary\n" +
                    "b: 22\n" +
                    "# position: 96, header: 2\n" +
                    "--- !!data #binary\n" +
                    "c: 3\n" +
                    "...\n" +
                    "# 130956 bytes remaining\n", table.dump());
           // System.out.println(table.dump());
        }
    }
}