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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

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
 * This test forces the fallback path (by passing a closed wire so {@code tryReserve} fails) and asserts
 * that the EOF marker lands at the padding-aligned position that the normal path produces.
 */
public class WriteEofFallbackTest extends QueueTestCommon {

    @Test
    public void fallbackEofIsWrittenAtNormalPathPaddedPosition() throws java.io.FileNotFoundException {
        assumeFalse(OS.isWindows());

        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
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

                // store.writePosition() is the start of the last record's header
                final long headerPosition = store.writePosition();

                // Force the fallback branch: a closed wire makes wire.bytes().tryReserve(..) return false.
                final Bytes<?> closedBytes = Bytes.allocateElasticOnHeap(64);
                final Wire closedWire = WireType.BINARY.apply(closedBytes);
                closedBytes.releaseLast(); // now closed -> tryReserve returns false
                assertFalse(closedWire.bytes().tryReserve(store));

                final boolean written = store.writeEOF(closedWire, 1_000);
                assertTrue("fallback writeEOF should succeed", written);

                try (final MappedBytes reader = MappedBytes.mappedBytes(store.file(), OS.pageSize())) {
                    final int header = reader.readVolatileInt(headerPosition);
                    final int len = Wires.lengthOf(header);
                    final long recordEnd = headerPosition + Wires.SPB_HEADER_SIZE + len;
                    final long paddedEnd = recordEnd + BytesUtil.padOffset(recordEnd);

                    // sanity: the record end must be unaligned for this test to be meaningful
                    assumeTrue("record end must be unaligned to exercise the padding difference",
                            paddedEnd != recordEnd);

                    // The EOF marker must be at the padding-aligned position, matching the normal path,
                    // and must NOT be at the unpadded record end (the pre-fix behaviour).
                    assertTrue("EOF marker must be written at the padding-aligned position " + paddedEnd,
                            Wires.isEndOfFile(reader.readVolatileInt(paddedEnd)));
                    assertFalse("EOF marker must NOT be written at the unpadded record end " + recordEnd,
                            Wires.isEndOfFile(reader.readVolatileInt(recordEnd)));
                }
            }
        }
    }
}
