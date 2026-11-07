/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.wire.Wires;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * QUEUE-148. A record in a table store declares its own length. If that length does not fit inside
 * the file, {@link SingleTableStore#acquireValueFor} must report the corruption. It must not walk
 * past the end of the mapped region.
 */
public class SingleTableStoreCorruptRecordTest extends QueueTestCommon {

    private static final int LENGTH_MASK = 0x3FFFFFFF;

    /**
     * The check must not reject a record that the table store wrote itself.
     */
    @Test
    public void anIntactTableStoreReadsBackEveryKeyItWrote() {
        File file = newTableStoreFile();
        try (TableStore<Metadata.NoMeta> store = build(file)) {
            for (int i = 0; i < 200; i++)
                try (LongValue value = store.acquireValueFor("key." + i, i)) {
                    assertEquals(i, value.getValue());
                }
        }
        try (TableStore<Metadata.NoMeta> store = build(file)) {
            try (LongValue value = store.acquireValueFor("key.199", -1L)) {
                assertEquals(199, value.getValue());
            }
        }
    }

    /**
     * A record whose declared length runs past the read limit must be reported as corruption.
     */
    @Test
    public void aRecordLengthThatDoesNotFitIsReportedAsCorruption() throws IOException {
        File file = newTableStoreFile();
        try (TableStore<Metadata.NoMeta> store = build(file)) {
            try (LongValue value = store.acquireValueFor("key.one", 1L)) {
                assertNotNull(value);
            }
        }
        overstateLengthOfFirstRecord(file);

        try (TableStore<Metadata.NoMeta> store = build(file)) {
            try (LongValue value = store.acquireValueFor("key.one", 1L)) {
                fail("a record that runs past the read limit was accepted: " + value);
            }
        } catch (CorruptTableStoreException expected) {
            assertTrue("message was " + expected.getMessage(),
                    expected.getMessage().contains("does not fit inside the read limit"));
        }
    }

    /**
     * Overstates the declared length of the first record that follows the first header. The flag
     * bits of the header keep their value, so the record still reads as a complete data record.
     */
    private void overstateLengthOfFirstRecord(File file) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            final long recordAt = Integer.BYTES + Wires.lengthOf(readIntAt(raf, 0));
            final int header = readIntAt(raf, recordAt);
            assertTrue("the record at " + recordAt + " is not a complete data record",
                    Wires.isReady(header) && Wires.isData(header));
            writeIntAt(raf, recordAt, (header & ~LENGTH_MASK) | LENGTH_MASK);
        }
    }

    private int readIntAt(RandomAccessFile raf, long position) throws IOException {
        byte[] b = new byte[Integer.BYTES];
        raf.seek(position);
        raf.readFully(b);
        return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private void writeIntAt(RandomAccessFile raf, long position, int value) throws IOException {
        raf.seek(position);
        raf.write(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array());
    }

    private File newTableStoreFile() {
        File dir = getTmpDir();
        dir.mkdirs();
        return new File(dir, "corrupt-record" + SingleTableStore.SUFFIX);
    }

    private TableStore<Metadata.NoMeta> build(File file) {
        return SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build();
    }
}
