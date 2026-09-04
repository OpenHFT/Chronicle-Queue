/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.wire.BinaryWireCode;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * QUEUE-148. A record in a table store declares its own length. Both acquisition and iteration must
 * bound every read by that declaration, then verify that the key, value and explicit Wire padding
 * consume it exactly.
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

            Map<String, Long> entries = new LinkedHashMap<>();
            store.forEachKey(entries, (result, key, value) -> result.put(key.toString(), value.int64()));
            assertEquals(200, entries.size());
            assertEquals(Long.valueOf(0), entries.get("key.0"));
            assertEquals(Long.valueOf(199), entries.get("key.199"));

            List<Long> selectivelyReadValues = new ArrayList<>();
            store.forEachKey(selectivelyReadValues, (result, key, value) -> {
                if ("key.0".contentEquals(key) || "key.199".contentEquals(key))
                    result.add(value.int64());
            });
            assertEquals(2, selectivelyReadValues.size());
            assertEquals(Long.valueOf(0), selectivelyReadValues.get(0));
            assertEquals(Long.valueOf(199), selectivelyReadValues.get(1));

            List<String> keysSeenWithoutReadingValues = new ArrayList<>();
            store.forEachKey(keysSeenWithoutReadingValues, (result, key, value) -> result.add(key.toString()));
            assertEquals(200, keysSeenWithoutReadingValues.size());
            assertEquals("key.0", keysSeenWithoutReadingValues.get(0));
            assertEquals("key.199", keysSeenWithoutReadingValues.get(199));
        }
    }

    /**
     * A positive length that cuts through the key/value cannot borrow bytes beyond its boundary.
     */
    @Test
    public void shorterPositiveRecordIsRejected() throws IOException {
        assertEveryTraversalRejects(LengthMutation.SHORTER_POSITIVE);
    }

    /**
     * A length can fit in the mapping and still be corrupt when it includes bytes after the value.
     */
    @Test
    public void longerInRangeRecordIsRejected() throws IOException {
        assertEveryTraversalRejects(LengthMutation.LONGER_IN_RANGE);
    }

    /**
     * A declared end outside the readable mapping must be rejected before any record parsing.
     */
    @Test
    public void recordBeyondTheReadLimitIsRejected() throws IOException {
        assertEveryTraversalRejects(LengthMutation.BEYOND_READ_LIMIT,
                "does not fit inside the read limit");
    }

    @Test
    public void laterCorruptRecordIsRejectedWhenVisited() throws IOException {
        File file = tableStoreWithTwoKeys();
        mutateRecordLength(file, 1, LengthMutation.SHORTER_POSITIVE);

        assertAcquireReads(file, "key.one", 1L);
        assertAcquireRejects(file, "key.two");
        assertAcquireRejects(file, "missing.key");
        assertForEachKeyRejects(file);
    }

    @Test
    public void laterMetadataHeaderIsRejected() throws IOException {
        File file = tableStoreWithTwoKeys();
        markRecordAsMetadata(file, 1);

        assertAcquireReads(file, "key.one", 1L);
        assertAcquireRejects(file, "key.two");
        assertAcquireRejects(file, "missing.key");
        assertForEachKeyRejects(file);
    }

    @Test
    public void nonLongValueCodeIsRejectedByEveryTraversal() throws IOException {
        File file = tableStoreWithTwoKeys();
        replaceFirstValueCode(file, BinaryWireCode.FLOAT64);

        assertAcquireRejects(file, "key.one");
        assertAcquireRejects(file, "key.two");
        assertForEachKeyRejects(file);
    }

    @Test
    public void malformedBinaryPaddingIsRejected() throws IOException {
        File truncated = tableStoreWithTwoKeys();
        final long truncatedValueCodeAt = replaceFirstValueCode(truncated, BinaryWireCode.PADDING32);
        truncateFirstRecordAfterValueCode(truncated, truncatedValueCodeAt);
        assertAcquireRejects(truncated, "key.one", "ends inside a padding header");
        assertForEachKeyRejects(truncated, "ends inside a padding header");

        File oversized = tableStoreWithTwoKeys();
        final long oversizedValueCodeAt = replaceFirstValueCode(oversized, BinaryWireCode.PADDING32);
        overwriteAfterValueCode(oversized, oversizedValueCodeAt, LENGTH_MASK);
        assertAcquireRejects(oversized, "key.one", "padding bytes with only");
        assertForEachKeyRejects(oversized, "padding bytes with only");
    }

    @Test
    @SuppressWarnings("deprecation")
    public void supportedBinaryWireTypesRoundTripBoundValues() {
        final WireType[] supported = {
                WireType.BINARY,
                WireType.BINARY_LIGHT,
                WireType.FIELDLESS_BINARY,
                WireType.COMPRESSED_BINARY
        };
        for (WireType wireType : supported)
            assertWireTypeRoundTripsBoundValues(wireType);
    }

    @Test
    public void rawTableStoreStillScansValuesWrittenDuringItsInitialOpen() {
        File file = newTableStoreFile("supported-raw");
        try (TableStore<Metadata.NoMeta> store = SingleTableBuilder
                .builder(file, WireType.RAW, Metadata.NoMeta.INSTANCE)
                .build();
             LongValue one = store.acquireValueFor("raw.one", 1L);
             LongValue two = store.acquireValueFor("raw.two", 2L);
             LongValue oneAgain = store.acquireValueFor("raw.one", -1L)) {
            assertEquals(1L, one.getValue());
            assertEquals(2L, two.getValue());
            assertEquals(1L, oneAgain.getValue());
        }
    }

    @Test
    public void rawRecordLengthsAreBoundedDuringTheInitialOpen() throws IOException {
        assertRawMutationRejected(LengthMutation.SHORTER_POSITIVE);
        assertRawMutationRejected(LengthMutation.LONGER_IN_RANGE);
    }

    @Test
    public void unsupportedWireTypesAreRejectedBeforeCreatingAFile() {
        final WireType[] unsupported = {
                WireType.TEXT,
                WireType.JSON,
                WireType.JSON_ONLY,
                WireType.JSONL,
                WireType.YAML,
                WireType.YAML_ONLY,
                WireType.CSV,
                WireType.READ_ANY
        };
        for (WireType wireType : unsupported) {
            final File file = newTableStoreFile("unsupported-" + wireType.name().toLowerCase());
            final IllegalArgumentException thrown = org.junit.Assert.assertThrows(IllegalArgumentException.class,
                    () -> SingleTableBuilder.builder(file, wireType, Metadata.NoMeta.INSTANCE).build());
            assertTrue("message was " + thrown.getMessage(), thrown.getMessage().contains(wireType.name()));
            assertFalse("unsupported wire type created " + file, file.exists());
        }
    }

    private void assertWireTypeRoundTripsBoundValues(WireType wireType) {
        File file = newTableStoreFile("supported-" + wireType.name().toLowerCase());
        try (TableStore<Metadata.NoMeta> store = SingleTableBuilder
                .builder(file, wireType, Metadata.NoMeta.INSTANCE)
                .build();
             LongValue one = store.acquireValueFor("bound.one", 1L);
             LongValue two = store.acquireValueFor("bound.two", 2L)) {
            assertEquals(1L, one.getValue());
            assertEquals(2L, two.getValue());
        }
        try (TableStore<Metadata.NoMeta> store = SingleTableBuilder
                .builder(file, wireType, Metadata.NoMeta.INSTANCE)
                .build();
             LongValue one = store.acquireValueFor("bound.one", -1L);
             LongValue two = store.acquireValueFor("bound.two", -1L)) {
            assertEquals(1L, one.getValue());
            assertEquals(2L, two.getValue());
        }
    }

    private void assertRawMutationRejected(LengthMutation mutation) throws IOException {
        File file = newTableStoreFile("raw-" + mutation.name().toLowerCase());
        try (TableStore<Metadata.NoMeta> store = SingleTableBuilder
                .builder(file, WireType.RAW, Metadata.NoMeta.INSTANCE)
                .build()) {
            try (LongValue one = store.acquireValueFor("raw.one", 1L);
                 LongValue two = store.acquireValueFor("raw.two", 2L)) {
                assertNotNull(one);
                assertNotNull(two);
            }
            mutateRecordLength(file, 0, mutation);
            final byte[] before = Files.readAllBytes(file.toPath());

            assertAcquireRejects(store, "raw.one");
            assertForEachKeyRejects(store);

            assertEquals("rejecting a malformed RAW record changed the file length",
                    before.length, Files.size(file.toPath()));
            org.junit.Assert.assertArrayEquals("rejecting a malformed RAW record changed its bytes",
                    before, Files.readAllBytes(file.toPath()));
        }
    }

    private void assertEveryTraversalRejects(LengthMutation mutation) throws IOException {
        assertEveryTraversalRejects(mutation, "the record at");
    }

    private void assertEveryTraversalRejects(LengthMutation mutation, String expectedMessage) throws IOException {
        File file = tableStoreWithTwoKeys();
        mutateRecordLength(file, 0, mutation);

        assertAcquireRejects(file, "key.one", expectedMessage);
        assertAcquireRejects(file, "key.two", expectedMessage);
        assertAcquireRejects(file, "missing.key", expectedMessage);
        assertForEachKeyRejects(file, expectedMessage);
    }

    private void assertAcquireReads(File file, String key, long expected) {
        try (TableStore<Metadata.NoMeta> store = build(file);
             LongValue value = store.acquireValueFor(key, -1L)) {
            assertEquals(expected, value.getValue());
        }
    }

    private void assertAcquireRejects(File file, String key) {
        assertAcquireRejects(file, key, "the record at");
    }

    private void assertAcquireRejects(File file, String key, String expectedMessage) {
        try (TableStore<Metadata.NoMeta> store = build(file);
             LongValue value = store.acquireValueFor(key, -1L)) {
            fail("acquireValueFor(" + key + ") accepted a corrupt record and returned " + value);
        } catch (CorruptTableStoreException expected) {
            assertTrue("message was " + expected.getMessage(), expected.getMessage().contains(expectedMessage));
        }
    }

    private void assertAcquireRejects(TableStore<Metadata.NoMeta> store, String key) {
        try (LongValue value = store.acquireValueFor(key, -1L)) {
            fail("acquireValueFor(" + key + ") accepted a corrupt record and returned " + value);
        } catch (CorruptTableStoreException expected) {
            assertTrue("message was " + expected.getMessage(), expected.getMessage().contains("record at"));
        }
    }

    private void assertForEachKeyRejects(File file) {
        assertForEachKeyRejects(file, "the record at");
    }

    private void assertForEachKeyRejects(File file, String expectedMessage) {
        try (TableStore<Metadata.NoMeta> store = build(file)) {
            assertForEachKeyRejects(store, expectedMessage);
        }
    }

    private void assertForEachKeyRejects(TableStore<Metadata.NoMeta> store) {
        assertForEachKeyRejects(store, "record at");
    }

    private void assertForEachKeyRejects(TableStore<Metadata.NoMeta> store, String expectedMessage) {
        try {
            store.forEachKey(new ArrayList<>(), (keys, key, value) -> keys.add(key.toString()));
            fail("forEachKey accepted a corrupt record");
        } catch (CorruptTableStoreException expected) {
            assertTrue("message was " + expected.getMessage(), expected.getMessage().contains(expectedMessage));
        }
    }

    private File tableStoreWithTwoKeys() {
        File file = newTableStoreFile();
        try (TableStore<Metadata.NoMeta> store = build(file);
             LongValue one = store.acquireValueFor("key.one", 1L);
             LongValue two = store.acquireValueFor("key.two", 2L)) {
            assertNotNull(one);
            assertNotNull(two);
        }
        return file;
    }

    private void mutateRecordLength(File file, int ordinal, LengthMutation mutation) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            final long recordAt = recordAt(raf, ordinal);
            final int header = readIntAt(raf, recordAt);
            assertTrue("the record at " + recordAt + " is not a complete data record",
                    Wires.isReady(header) && Wires.isData(header));
            final int originalLength = Wires.lengthOf(header);
            final int replacementLength;
            switch (mutation) {
                case SHORTER_POSITIVE:
                    assertTrue("record is too short to shorten positively: " + originalLength, originalLength > 1);
                    replacementLength = 1;
                    break;
                case LONGER_IN_RANGE:
                    replacementLength = originalLength + 1;
                    assertTrue("the longer record would exceed the file", recordAt + Integer.BYTES + replacementLength <= raf.length());
                    break;
                case BEYOND_READ_LIMIT:
                    replacementLength = LENGTH_MASK;
                    break;
                default:
                    throw new AssertionError(mutation);
            }
            writeIntAt(raf, recordAt, (header & ~LENGTH_MASK) | replacementLength);
        }
    }

    private void markRecordAsMetadata(File file, int ordinal) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            final long recordAt = recordAt(raf, ordinal);
            final int header = readIntAt(raf, recordAt);
            assertTrue("the record at " + recordAt + " is not a complete data record",
                    Wires.isReady(header) && Wires.isData(header));
            writeIntAt(raf, recordAt, header | Wires.META_DATA);
        }
    }

    private long replaceFirstValueCode(File file, int replacementCode) throws IOException {
        final long valueCodeAt = firstValueCodeAt(file);
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(valueCodeAt);
            final int originalCode = raf.read();
            assertTrue("the table-store value did not use a bound-long code: 0x"
                            + Integer.toHexString(originalCode),
                    originalCode == 0 || originalCode == BinaryWireCode.INT64);
            raf.seek(valueCodeAt);
            raf.write(replacementCode);
        }
        return valueCodeAt;
    }

    private void overwriteAfterValueCode(File file, long valueCodeAt, int value) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            writeIntAt(raf, valueCodeAt + 1, value);
        }
    }

    private void truncateFirstRecordAfterValueCode(File file, long valueCodeAt) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            final long recordAt = recordAt(raf, 0);
            final int header = readIntAt(raf, recordAt);
            final int lengthThroughValueCode = Math.toIntExact(valueCodeAt - recordAt - Integer.BYTES + 1);
            writeIntAt(raf, recordAt, (header & ~LENGTH_MASK) | lengthThroughValueCode);
        }
    }

    private long firstValueCodeAt(File file) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            final long recordAt = recordAt(raf, 0);
            final int header = readIntAt(raf, recordAt);
            try (MappedBytes bytes = MappedBytes.mappedBytes(
                    file, OS.SAFE_PAGE_SIZE, OS.SAFE_PAGE_SIZE, true)) {
                bytes.singleThreadedCheckDisabled(true);
                try {
                    final Wire wire = WireType.BINARY_LIGHT.apply(bytes);
                    bytes.readPositionRemaining(recordAt + Integer.BYTES, Wires.lengthOf(header));
                    wire.readEventName(new StringBuilder());
                    wire.consumePadding();
                    return bytes.readPosition();
                } finally {
                    bytes.singleThreadedCheckReset();
                }
            }
        }
    }

    private long recordAt(RandomAccessFile raf, int ordinal) throws IOException {
        long recordAt = alignHeader(Integer.BYTES + Wires.lengthOf(readIntAt(raf, 0)));
        for (int i = 0; i < ordinal; i++)
            recordAt = alignHeader(recordAt + Integer.BYTES + Wires.lengthOf(readIntAt(raf, recordAt)));
        return recordAt;
    }

    private long alignHeader(long position) {
        return (position + Integer.BYTES - 1L) & -Integer.BYTES;
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
        return newTableStoreFile("corrupt-record");
    }

    private File newTableStoreFile(String stem) {
        File dir = getTmpDir();
        dir.mkdirs();
        return new File(dir, stem + SingleTableStore.SUFFIX);
    }

    private TableStore<Metadata.NoMeta> build(File file) {
        return SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build();
    }

    private enum LengthMutation {
        SHORTER_POSITIVE,
        LONGER_IN_RANGE,
        BEYOND_READ_LIMIT
    }
}
