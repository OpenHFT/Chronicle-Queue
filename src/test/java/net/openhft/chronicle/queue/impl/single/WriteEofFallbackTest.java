/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

/**
 * Regression test for https://github.com/OpenHFT/Chronicle-Queue/issues/1096
 * <p>
 * {@link SingleChronicleQueueStore#writeEOF(Wire, long)} has two branches: the normal path uses the
 * already-mapped wire, and a fallback path (taken when the byte reservation fails) that maps a fresh
 * {@link MappedBytes}. The fallback used to construct its wire without setting {@code usePadding},
 * whereas the normal-path wire always uses padding for the current data format ({@code dataVersion > 0}).
 * As a consequence the EOF marker was written at an unpadded offset in the fallback path, so the byte
 * layout of the file depended purely on whether the reservation happened to succeed.
 * <p>
 * This test builds equivalent stores, exercises both branches, and compares their
 * resulting positions, file lengths, EOF offsets, and surrounding bytes.
 */
public class WriteEofFallbackTest extends QueueTestCommon {

    @Test
    public void fallbackAndNormalEofPathsProduceEquivalentStores() throws IOException {
        assumeFalse(OS.isWindows());

        final EofOutcome normal = writeEof(getTmpDir(), false);
        final EofOutcome fallback = writeEof(getTmpDir(), true);

        assertTrue("record end must be unaligned to exercise the padding difference",
                normal.paddedEnd != normal.recordEnd);
        assertEquals(normal.recordEnd, fallback.recordEnd);
        assertEquals(normal.paddedEnd, fallback.paddedEnd);
        assertEquals("EOF offset", normal.eofOffset, fallback.eofOffset);
        assertEquals("resulting write position", normal.resultingWritePosition,
                fallback.resultingWritePosition);
        assertEquals("truncated file length", normal.fileLength, fallback.fileLength);
        assertArrayEquals("bytes around EOF", normal.bytesAroundEof, fallback.bytesAroundEof);
    }

    private EofOutcome writeEof(File directory, boolean forceFallback) throws IOException {
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .rollCycle(RollCycles.FAST_DAILY)
                .timeProvider(new SetTimeProvider())
                .testBlockSize()
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            // Append a single record whose payload length (5 bytes) is deliberately not a multiple of 4,
            // so that the record end is unaligned and padding bytes are required. This is the only case
            // where the missing usePadding produces an observable difference.
            try (final DocumentContext dc = appender.writingDocument()) {
                dc.wire().bytes().writeInt(0x12345678).writeByte((byte) 0x9A);
            }

            final int cycle = queue.cycle();
            try (final SingleChronicleQueueStore store = queue.storeForCycle(cycle, queue.epoch(), false, null)) {
                final long headerPosition = store.writePosition();
                if (forceFallback) {
                    final Bytes<?> closedBytes = Bytes.allocateElasticOnHeap(64);
                    final Wire closedWire = WireType.BINARY.apply(closedBytes);
                    closedBytes.releaseLast();
                    assertFalse(closedWire.bytes().tryReserve(store));
                    assertTrue("fallback writeEOF should succeed", store.writeEOF(closedWire, 1_000));
                } else {
                    try (final MappedBytes normalBytes = MappedBytes.mappedBytes(store.file(), OS.pageSize())) {
                        final Wire normalWire = WireType.BINARY.apply(normalBytes);
                        normalWire.usePadding(store.dataVersion() > 0);
                        assertTrue("normal writeEOF should succeed", store.writeEOF(normalWire, 1_000));
                    }
                }

                try (final MappedBytes reader = MappedBytes.mappedBytes(store.file(), OS.pageSize())) {
                    final int header = reader.readVolatileInt(headerPosition);
                    final int len = Wires.lengthOf(header);
                    final long recordEnd = headerPosition + Wires.SPB_HEADER_SIZE + len;
                    final long paddedEnd = recordEnd + BytesUtil.padOffset(recordEnd);
                    final long eofOffset = findEof(reader, recordEnd);
                    final long windowStart = recordEnd - 8;
                    final byte[] bytesAroundEof = new byte[32];
                    for (int i = 0; i < bytesAroundEof.length; i++)
                        bytesAroundEof[i] = reader.readByte(windowStart + i);

                    return new EofOutcome(recordEnd, paddedEnd, eofOffset,
                            eofOffset + Wires.SPB_HEADER_SIZE, store.file().length(), bytesAroundEof);
                }
            }
        }
    }

    private static long findEof(MappedBytes bytes, long recordEnd) {
        for (long position = recordEnd; position < recordEnd + 16; position++) {
            if (Wires.isEndOfFile(bytes.readVolatileInt(position)))
                return position;
        }
        throw new AssertionError("EOF not found after record end " + recordEnd);
    }

    private static final class EofOutcome {
        final long recordEnd;
        final long paddedEnd;
        final long eofOffset;
        final long resultingWritePosition;
        final long fileLength;
        final byte[] bytesAroundEof;

        EofOutcome(long recordEnd, long paddedEnd, long eofOffset,
                   long resultingWritePosition, long fileLength, byte[] bytesAroundEof) {
            this.recordEnd = recordEnd;
            this.paddedEnd = paddedEnd;
            this.eofOffset = eofOffset;
            this.resultingWritePosition = resultingWritePosition;
            this.fileLength = fileLength;
            this.bytesAroundEof = Arrays.copyOf(bytesAroundEof, bytesAroundEof.length);
        }
    }
}
