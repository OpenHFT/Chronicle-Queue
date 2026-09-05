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
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
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
        assertEveryTraversalRejects(LengthMutation.LONGER_IN_RANGE, "content ends at");
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
    public void firstRecordMustBeMetadata() throws IOException {
        final File file = newTableStoreFile("first-record-is-data");
        try (TableStore<Metadata.NoMeta> store = build(file)) {
            try (LongValue value = store.acquireValueFor("key.one", 1L)) {
                assertEquals(1L, value.getValue());
            }
            markFirstHeaderAsData(file);

            assertAcquireRejects(store, "key.one", "first record is not table-store metadata");
            assertForEachKeyRejects(store, "first record is not table-store metadata");
        }
    }

    @Test
    public void firstMetadataLengthMustBePositive() throws IOException {
        final File file = newTableStoreFile("zero-length-metadata");
        try (TableStore<Metadata.NoMeta> store = build(file)) {
            try (LongValue value = store.acquireValueFor("key.one", 1L)) {
                assertEquals(1L, value.getValue());
            }
            rewriteFirstHeaderLength(file, 0);

            assertAcquireRejects(store, "key.one", "metadata header has no body");
            assertForEachKeyRejects(store, "metadata header has no body");
        }
    }

    @Test
    public void firstMetadataEndMustBeHeaderAligned() throws IOException {
        final File file = newTableStoreFile("misaligned-metadata-end");
        try (TableStore<Metadata.NoMeta> store = build(file)) {
            try (LongValue value = store.acquireValueFor("key.one", 1L)) {
                assertEquals(1L, value.getValue());
            }
            final int originalLength;
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                originalLength = Wires.lengthOf(readIntAt(raf, 0L));
            }
            rewriteFirstHeaderLength(file, originalLength + 1);

            assertAcquireRejects(store, "key.one", "metadata header end is not aligned");
            assertForEachKeyRejects(store, "metadata header end is not aligned");
        }
    }

    @Test
    public void missingEventNameIsRejectedByEveryTraversal() throws IOException {
        for (WireType wireType : new WireType[]{WireType.BINARY_LIGHT, WireType.BINARY}) {
            final File file = tableStoreWithTwoKeys(wireType);
            eraseFirstEventName(file, wireType);

            assertAcquireRejects(file, wireType, "key.one", "event-name token");
            assertAcquireRejects(file, wireType, "key.two", "event-name token");
            assertAcquireRejects(file, wireType, "missing.key", "event-name token");
            assertForEachKeyRejectsBeforeCallback(file, wireType, "event-name token");
        }
    }

    @Test
    public void nonCanonicalFieldNameTokenIsRejectedByEveryTraversal() throws IOException {
        for (WireType wireType : new WireType[]{WireType.BINARY_LIGHT, WireType.BINARY}) {
            final File file = tableStoreWithTwoKeys(wireType);
            replaceFirstEventToken(file, BinaryWireCode.FIELD_NAME_ANY);

            assertAcquireRejects(file, wireType, "key.one", "event-name token");
            assertAcquireRejects(file, wireType, "key.two", "event-name token");
            assertAcquireRejects(file, wireType, "missing.key", "event-name token");
            assertForEachKeyRejectsBeforeCallback(file, wireType, "event-name token");
        }
    }

    @Test
    public void explicitEmptyEventNameRemainsValid() {
        for (WireType wireType : new WireType[]{WireType.BINARY_LIGHT, WireType.BINARY}) {
            final File file = newTableStoreFile("empty-key-" + wireType.name().toLowerCase());
            try (TableStore<Metadata.NoMeta> store = build(file, wireType);
                 LongValue value = store.acquireValueFor("", 17L)) {
                assertEquals(17L, value.getValue());
            }

            try (TableStore<Metadata.NoMeta> store = build(file, wireType);
                 LongValue value = store.acquireValueFor("", -1L)) {
                assertEquals(17L, value.getValue());
                final Map<String, Long> entries = new LinkedHashMap<>();
                store.forEachKey(entries, (result, key, valueIn) -> result.put(key.toString(), valueIn.int64()));
                assertEquals(Long.valueOf(17L), entries.get(""));
            }
        }
    }

    @Test
    public void misalignedBoundLongIsRejected() throws IOException {
        final File file = tableStoreWithTwoKeys();
        shiftFirstBoundLongOneByteEarlier(file);

        assertAcquireRejects(file, "key.one", "unaligned address");
        assertAcquireRejects(file, "key.two", "unaligned address");
        assertForEachKeyRejects(file, "unaligned address");
    }

    @Test
    public void recordEndMustBeHeaderAligned() throws IOException {
        final File file = tableStoreWithOneKey();
        extendFirstRecordWithPadding(file);

        assertAcquireRejects(file, "key.one", "not aligned for the next header");
        assertAcquireRejects(file, "missing.key", "not aligned for the next header");
        assertForEachKeyRejects(file, "not aligned for the next header");
    }

    @Test
    public void nonLongValueCodeIsRejectedByEveryTraversal() throws IOException {
        File file = tableStoreWithTwoKeys();
        replaceFirstValueCode(file, BinaryWireCode.FLOAT64);

        assertAcquireRejects(file, "key.one");
        assertAcquireRejects(file, "key.two");
        assertForEachKeyRejectsBeforeCallback(file, WireType.BINARY_LIGHT, "the record at");
    }

    @Test
    public void malformedBinaryPaddingIsRejected() throws IOException {
        File truncated = tableStoreWithTwoKeys();
        final long truncatedValueCodeAt = replaceFirstValueCode(truncated, BinaryWireCode.PADDING32);
        truncateFirstRecordAfterValueCode(truncated, truncatedValueCodeAt);
        assertAcquireRejects(truncated, "key.one", "ends inside a padding header");
        assertForEachKeyRejectsBeforeCallback(
                truncated, WireType.BINARY_LIGHT, "ends inside a padding header");

        File oversized = tableStoreWithTwoKeys();
        final long oversizedValueCodeAt = replaceFirstValueCode(oversized, BinaryWireCode.PADDING32);
        overwriteAfterValueCode(oversized, oversizedValueCodeAt, LENGTH_MASK);
        assertAcquireRejects(oversized, "key.one", "padding bytes with only");
        assertForEachKeyRejectsBeforeCallback(
                oversized, WireType.BINARY_LIGHT, "padding bytes with only");
    }

    @Test
    @SuppressWarnings("deprecation")
    public void canonicalAndLegacyBinarySelectorsRoundTripTheCommonSubset() {
        final WireType[] supported = {
                WireType.BINARY_LIGHT,
                WireType.BINARY
        };
        for (WireType wireType : supported)
            assertWireTypeRoundTripsBoundValues(wireType);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void unsupportedWritableWireTypesFailBeforeCreatingAFile() {
        final WireType[] unsupported = {
                WireType.TEXT,
                WireType.JSON,
                WireType.JSON_ONLY,
                WireType.JSONL,
                WireType.YAML,
                WireType.YAML_ONLY,
                WireType.CSV,
                WireType.READ_ANY,
                WireType.RAW,
                WireType.FIELDLESS_BINARY,
                WireType.COMPRESSED_BINARY
        };
        for (WireType wireType : unsupported) {
            final File file = newTableStoreFile("unsupported-" + wireType.name().toLowerCase());
            final IllegalArgumentException thrown = org.junit.Assert.assertThrows(IllegalArgumentException.class,
                    () -> SingleTableBuilder.builder(file, wireType, Metadata.NoMeta.INSTANCE).build());
            assertTrue("message was " + thrown.getMessage(), thrown.getMessage().contains(wireType.name()));
            assertFalse("unsupported wire type created " + file, file.exists());
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void readAnyReopensExistingBinaryTableStore() {
        final WireType[] readable = {
                WireType.BINARY_LIGHT,
                WireType.BINARY
        };
        for (WireType wireType : readable) {
            final File file = newTableStoreFile("read-any-" + wireType.name().toLowerCase());
            try (TableStore<Metadata.NoMeta> store = build(file, wireType);
                 LongValue value = store.acquireValueFor("read.any", 23L)) {
                assertEquals(23L, value.getValue());
            }

            try (TableStore<Metadata.NoMeta> store = SingleTableBuilder
                    .builder(file, WireType.READ_ANY, Metadata.NoMeta.INSTANCE)
                    .readOnly(true)
                    .build();
                 LongValue value = store.acquireValueFor("read.any", -1L)) {
                assertEquals(23L, value.getValue());
            }
        }
    }

    @Test
    public void readOnlyStoreOpenedBeforeGrowthSeesLaterKeys() throws IOException {
        final File file = newTableStoreFile("read-only-observes-growth");
        try (TableStore<Metadata.NoMeta> writer = build(file, WireType.BINARY_LIGHT);
             TableStore<Metadata.NoMeta> reader = SingleTableBuilder
                     .builder(file, WireType.READ_ANY, Metadata.NoMeta.INSTANCE)
                     .readOnly(true)
                     .build()) {
            final long initialLength = Files.size(file.toPath());
            String lastKey = null;
            long lastValue = -1L;
            for (int i = 0; i < 20_000 && Files.size(file.toPath()) <= initialLength; i++) {
                lastKey = "growth.key." + i;
                lastValue = i;
                try (LongValue appended = writer.acquireValueFor(lastKey, lastValue)) {
                    assertEquals(lastValue, appended.getValue());
                }
            }
            assertTrue("fixture did not grow beyond its initial mapping", Files.size(file.toPath()) > initialLength);
            try (LongValue observed = reader.acquireValueFor(lastKey, -1L)) {
                assertEquals("read-only mapping did not observe the post-open key", lastValue, observed.getValue());
            }
        }
    }

    @Test
    public void readAnyIgnoresFieldCodeBytesInTheFirstHeader() throws IOException {
        File selected = null;
        for (int payloadLength = 0; payloadLength < 512 && selected == null; payloadLength++) {
            final File candidate = newTableStoreFile("read-any-header-code-" + payloadLength);
            try (TableStore<SizedMetadata> store = SingleTableBuilder
                    .builder(candidate, WireType.BINARY_LIGHT, new SizedMetadata(payloadLength))
                    .build();
                 LongValue value = store.acquireValueFor("read.any.header", 53L)) {
                assertEquals(53L, value.getValue());
            }
            try (RandomAccessFile raf = new RandomAccessFile(candidate, "r")) {
                final int firstHeaderByte = readIntAt(raf, 0L) & 0xff;
                if (BinaryWireCode.isFieldCode(firstHeaderByte))
                    selected = candidate;
            }
        }
        assertNotNull("could not generate a valid first-header length whose low byte is a field code", selected);

        try (TableStore<SizedMetadata> store = SingleTableBuilder
                .builder(selected, WireType.READ_ANY, new SizedMetadata(0))
                .readOnly(true)
                .build();
             LongValue value = store.acquireValueFor("read.any.header", -1L)) {
            assertEquals(53L, value.getValue());
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void canonicalAndLegacySelectorsCrossOpenWritableAndReadOnlyStores() {
        final WireType[] binaryFormats = {WireType.BINARY_LIGHT, WireType.BINARY};
        for (WireType persisted : binaryFormats) {
            final File file = newTableStoreFile("cross-open-" + persisted.name().toLowerCase());
            try (TableStore<Metadata.NoMeta> store = build(file, persisted);
                 LongValue value = store.acquireValueFor("cross.open", 31L)) {
                assertEquals(31L, value.getValue());
            }

            for (WireType selector : binaryFormats) {
                assertCrossOpenReads(file, selector, false);
                assertCrossOpenReads(file, selector, true);
            }
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

    private void assertCrossOpenReads(File file, WireType selector, boolean readOnly) {
        try (TableStore<Metadata.NoMeta> store = SingleTableBuilder
                .builder(file, selector, Metadata.NoMeta.INSTANCE)
                .readOnly(readOnly)
                .build();
             LongValue value = store.acquireValueFor("cross.open", -1L)) {
            assertEquals("selector=" + selector + ", readOnly=" + readOnly, 31L, value.getValue());
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
        assertAcquireRejects(file, WireType.BINARY_LIGHT, key, expectedMessage);
    }

    private void assertAcquireRejects(File file, WireType wireType, String key, String expectedMessage) {
        try (TableStore<Metadata.NoMeta> store = build(file, wireType);
             LongValue value = store.acquireValueFor(key, -1L)) {
            fail("acquireValueFor(" + key + ") accepted a corrupt record and returned " + value);
        } catch (CorruptTableStoreException expected) {
            assertTrue("message was " + expected.getMessage(), expected.getMessage().contains(expectedMessage));
        }
    }

    private void assertAcquireRejects(TableStore<Metadata.NoMeta> store, String key) {
        assertAcquireRejects(store, key, "record at");
    }

    private void assertAcquireRejects(TableStore<Metadata.NoMeta> store, String key, String expectedMessage) {
        try (LongValue value = store.acquireValueFor(key, -1L)) {
            fail("acquireValueFor(" + key + ") accepted a corrupt record and returned " + value);
        } catch (CorruptTableStoreException expected) {
            assertTrue("message was " + expected.getMessage(), expected.getMessage().contains(expectedMessage));
        }
    }

    private void assertForEachKeyRejects(File file) {
        assertForEachKeyRejects(file, "the record at");
    }

    private void assertForEachKeyRejects(File file, String expectedMessage) {
        assertForEachKeyRejects(file, WireType.BINARY_LIGHT, expectedMessage);
    }

    private void assertForEachKeyRejects(File file, WireType wireType, String expectedMessage) {
        try (TableStore<Metadata.NoMeta> store = build(file, wireType)) {
            assertForEachKeyRejects(store, expectedMessage);
        }
    }

    private void assertForEachKeyRejectsBeforeCallback(File file,
                                                       WireType wireType,
                                                       String expectedMessage) {
        try (TableStore<Metadata.NoMeta> store = build(file, wireType)) {
            final List<String> callbacks = new ArrayList<>();
            try {
                store.forEachKey(callbacks, (keys, key, value) -> keys.add(key.toString()));
                fail("forEachKey accepted a corrupt record");
            } catch (CorruptTableStoreException expected) {
                assertTrue("message was " + expected.getMessage(),
                        expected.getMessage().contains(expectedMessage));
                assertTrue("callback ran before the first corrupt record was validated: " + callbacks,
                        callbacks.isEmpty());
            }
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
        return tableStoreWithTwoKeys(WireType.BINARY_LIGHT);
    }

    private File tableStoreWithTwoKeys(WireType wireType) {
        File file = newTableStoreFile("two-keys-" + wireType.name().toLowerCase());
        try (TableStore<Metadata.NoMeta> store = build(file, wireType);
             LongValue one = store.acquireValueFor("key.one", 1L);
             LongValue two = store.acquireValueFor("key.two", 2L)) {
            assertNotNull(one);
            assertNotNull(two);
        }
        return file;
    }

    private File tableStoreWithOneKey() {
        File file = newTableStoreFile("one-key");
        try (TableStore<Metadata.NoMeta> store = build(file);
             LongValue one = store.acquireValueFor("key.one", 1L)) {
            assertNotNull(one);
        }
        return file;
    }

    private void eraseFirstEventName(File file, WireType wireType) throws IOException {
        final long valueCodeAt = firstValueCodeAt(file, wireType);
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            final long bodyStart = recordAt(raf, 0) + Integer.BYTES;
            assertTrue("the first record has no event-name bytes", bodyStart < valueCodeAt);
            raf.seek(bodyStart);
            for (long position = bodyStart; position < valueCodeAt; position++)
                raf.write(BinaryWireCode.PADDING);
        }
    }

    private void replaceFirstEventToken(File file, int replacementCode) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            final long bodyStart = recordAt(raf, 0) + Integer.BYTES;
            raf.seek(bodyStart);
            assertEquals("the first record does not begin with EVENT_NAME",
                    BinaryWireCode.EVENT_NAME, raf.read());
            raf.seek(bodyStart);
            raf.write(replacementCode);
        }
    }

    private void shiftFirstBoundLongOneByteEarlier(File file) throws IOException {
        final long valueCodeAt = firstValueCodeAt(file);
        final long eventEnd = firstValuePosition(file, WireType.BINARY_LIGHT, false);
        assertTrue("the test record has no padding before its bound long", eventEnd < valueCodeAt);
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(eventEnd);
            for (long position = eventEnd; position < valueCodeAt; position++)
                raf.write(BinaryWireCode.PADDING);
            raf.seek(valueCodeAt - 1L);
            assertEquals("the test requires one padding byte before the bound long",
                    BinaryWireCode.PADDING, raf.read());
            final byte[] codeAndValue = new byte[1 + Long.BYTES];
            raf.readFully(codeAndValue);
            assertTrue("the table-store value did not use a bound-long code",
                    (codeAndValue[0] & 0xff) == 0 || (codeAndValue[0] & 0xff) == BinaryWireCode.INT64);
            raf.seek(valueCodeAt - 1L);
            raf.write(codeAndValue);
            raf.write(BinaryWireCode.PADDING);
        }
    }

    private void extendFirstRecordWithPadding(File file) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            final long recordAt = recordAt(raf, 0);
            final int header = readIntAt(raf, recordAt);
            final int originalLength = Wires.lengthOf(header);
            raf.seek(recordAt + Integer.BYTES + originalLength);
            raf.write(BinaryWireCode.PADDING);
            writeIntAt(raf, recordAt, (header & ~LENGTH_MASK) | (originalLength + 1));
        }
    }

    private void markFirstHeaderAsData(File file) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            final int header = readIntAt(raf, 0L);
            assertTrue("the first record is not a ready metadata header",
                    Wires.isReady(header) && (header & Wires.META_DATA) != 0);
            writeIntAt(raf, 0L, header & ~Wires.META_DATA);
        }
    }

    private void rewriteFirstHeaderLength(File file, int length) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            final int header = readIntAt(raf, 0L);
            assertTrue("the first record is not a ready metadata header",
                    Wires.isReady(header) && (header & Wires.META_DATA) != 0);
            writeIntAt(raf, 0L, (header & ~LENGTH_MASK) | length);
        }
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
                    // Keep the end aligned so exact consumption, rather than header alignment, rejects the mutation.
                    replacementLength = originalLength + Integer.BYTES;
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
        return firstValueCodeAt(file, WireType.BINARY_LIGHT);
    }

    private long firstValueCodeAt(File file, WireType wireType) throws IOException {
        return firstValuePosition(file, wireType, true);
    }

    private long firstValuePosition(File file, WireType wireType, boolean consumePadding) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            final long recordAt = recordAt(raf, 0);
            final int header = readIntAt(raf, recordAt);
            try (MappedBytes bytes = MappedBytes.mappedBytes(
                    file, OS.SAFE_PAGE_SIZE, OS.SAFE_PAGE_SIZE, true)) {
                bytes.singleThreadedCheckDisabled(true);
                try {
                    final Wire wire = wireType.apply(bytes);
                    bytes.readPositionRemaining(recordAt + Integer.BYTES, Wires.lengthOf(header));
                    wire.readEventName(new StringBuilder());
                    if (consumePadding)
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
        return build(file, WireType.BINARY_LIGHT);
    }

    private TableStore<Metadata.NoMeta> build(File file, WireType wireType) {
        return SingleTableBuilder.builder(file, wireType, Metadata.NoMeta.INSTANCE).build();
    }

    private enum LengthMutation {
        SHORTER_POSITIVE,
        LONGER_IN_RANGE,
        BEYOND_READ_LIMIT
    }

    public static final class SizedMetadata implements Metadata {
        private final String payload;

        SizedMetadata(int payloadLength) {
            final char[] chars = new char[payloadLength];
            Arrays.fill(chars, 'x');
            payload = new String(chars);
        }

        @SuppressWarnings("unused")
        public SizedMetadata(@NotNull WireIn wire) {
            payload = wire.read("payload").text();
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write("payload").text(payload);
        }
    }
}
