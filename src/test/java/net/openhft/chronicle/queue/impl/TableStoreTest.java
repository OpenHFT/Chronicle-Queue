package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TableStoreTest {
    @Test
    public void acquireValueFor() {
        String file = OS.TARGET + "/table-" + System.nanoTime() + ".cq4t";
        new File(file).deleteOnExit();
        try (TableStore table = SingleTableBuilder.binary(file).build()) {

            LongValue a = table.acquireValueFor("a");
            LongValue b = table.acquireValueFor("b");
            assertEquals(Long.MIN_VALUE, a.getVolatileValue());
            assertTrue(a.compareAndSwapValue(Long.MIN_VALUE, 1));
            assertEquals(Long.MIN_VALUE, b.getVolatileValue());
            assertTrue(b.compareAndSwapValue(Long.MIN_VALUE, 2));
            assertEquals("--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 112, header: 0\n" +
                    "--- !!data #binary\n" +
                    "a: 1\n" +
                    "# position: 128, header: 1\n" +
                    "--- !!data #binary\n" +
                    "b: 2\n" +
                    "...\n" +
                    "# 65388 bytes remaining\n", table.dump());
        }

        try (TableStore table = SingleTableBuilder.binary(file).build()) {

            LongValue c = table.acquireValueFor("c");
            LongValue b = table.acquireValueFor("b");
            assertEquals(Long.MIN_VALUE, c.getVolatileValue());
            assertTrue(c.compareAndSwapValue(Long.MIN_VALUE, 3));
            assertEquals(2, b.getVolatileValue());
            assertTrue(b.compareAndSwapValue(2, 22));
            assertEquals("--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 112, header: 0\n" +
                    "--- !!data #binary\n" +
                    "a: 1\n" +
                    "# position: 128, header: 1\n" +
                    "--- !!data #binary\n" +
                    "b: 22\n" +
                    "# position: 144, header: 2\n" +
                    "--- !!data #binary\n" +
                    "c: 3\n" +
                    "...\n" +
                    "# 65372 bytes remaining\n", table.dump());
            System.out.println(table.dump());
        }
    }
}