/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl.single.work.in.progress;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.VanillaBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.values.LongArrayValues;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by peter.lawrey on 30/01/15.
 */
public class SingleTailer implements ExcerptTailer {
    @NotNull

    private final IndexedSingleChronicleQueue chronicle;
    private final VanillaBytes bytes = new VanillaBytes(Bytes.elasticByteBuffer());
    private final Wire wire;
    /**
     * reads an item in the index, the index is stored in meta data
     *
     * @param offset the address of the document
     * @param index  the index of of the array item
     * @return returns a long at array {@code index}
     */
    LongArrayValues values = null;
    private long index;
    private ThreadLocal<ByteableLongArrayValues> value;

    public SingleTailer(@NotNull final AbstractChronicle chronicle,
                        @NotNull final Function<Bytes, Wire> wireProvider,
                        WireType wireType) {
        this.chronicle = (IndexedSingleChronicleQueue) chronicle;
        this.wire = wireProvider.apply(bytes);
        this.value = Indexer.newLongArrayValuesPool(wireType);
        toStart();
    }


    public boolean readDocument(Consumer<WireIn> reader) {
        return wire.readDocument(null, wire1 -> reader.accept(wire1));
    }

    @Override
    public boolean readDocument(@NotNull ReadMarshallable reader) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public long index() {
        return 0;
    }

    @Override
    public long cycle() {
        return 0;
    }

    /**
     * The indexes are stored in many excerpts, so the index2index tells chronicle where ( in other
     * words the address of where ) the root first level index is stored. The indexing works like a
     * tree, but only 2 levels deep, the root of the tree is at index2index ( this first level index
     * is 1MB in size and there is only one of them, it only holds the addresses of the second level
     * indexes, there will be many second level indexes ( created on demand ), each is about 1MB in
     * size  (this second level index only stores the position of every 64th excerpt), so from every
     * 64th excerpt a linear scan occurs. The indexes are only built when the indexer is run, this
     * could be on a background thread. Each index is created into chronicle as an excerpt.
     */
    @Override
    public boolean index(final long index) {

        assert bytes.capacity() > 0;
        long writeByte = this.chronicle.header.getWriteByte();
        assert bytes.capacity() > 0;

        assert bytes.capacity() > 0;
        long pos = bytes.readPosition();
        long start = 0;

        long address = 0;
        long index2index = chronicle.indexToIndex();
        assert index2index != 0;

        long address1 = readIndexAt(index2index, Indexer.IndexOffset.toAddress0(index));
        long address2;

        if (address1 != 0) {

            address2 = readIndexAt(address1, Indexer.IndexOffset.toAddress1(index));

            if (address2 != 0) {
                wire.bytes().readPosition(address2);
                start = ((index / 64L)) * 64L;
                address = address2;
            } else {

                long lastKnownIndex = start;
                long lastKnownAddress = this.chronicle.firstBytes();
                for (long count = 0; count < ((int) (1L << 17L)); count++) {

                    address = readIndexAt(address1, count);

                    if (address != 0) {
                        if (count > 0) {
                            lastKnownIndex += (1L << (17L + 6L));
                            lastKnownAddress = address;
                        }
                    } else {
                        start = lastKnownIndex;
                        address = lastKnownAddress;
                        break;
                    }
                }

            }

        } else {
            long lastKnownIndex = 0;
            for (long count = 0; count < ((int) (1L << 17L)); count++) {


                address = readIndexAt(chronicle.indexToIndex(), count);

                if (address != 0) {
                    if (count > 0)
                        lastKnownIndex += (1L << (6L));
                } else {
                    start = lastKnownIndex;

                    break;
                }
            }

        }

        assert bytes.capacity() > 0;


        if (address != 0)
            bytes.readPosition(address);
        else
            // start to read just after the header
            bytes.readPosition(Header.PADDED_SIZE);

        bytes.readLimit(writeByte);
        long last = chronicle.lastIndex();

        // linear scan the last part
        for (long i = start; i < last; i++) {
            final long j = i;

            if (index == j)
                return true;

            try {
                readDocument((ReadMarshallable) wire -> {

                });
            } catch (IOException e) {
                Jvm.rethrow(e);
            }

        }

        wire.bytes().readPosition(pos);
        return false;

    }

    @Override
    public boolean index(long cycle, long index) throws IOException {
        return false;
    }

    @NotNull
    @Override
    public ExcerptTailer toStart() {
        index = -1;
        chronicle.index(-1L, bytes);
        return this;
    }

    @NotNull
    @Override
    public ExcerptTailer toEnd() {
        index(chronicle.lastIndex());
        return this;
    }

    private long readIndexAt(long offset, long index) {

        if (offset == 0)
            return 0;

        long pos = chronicle.bytes().readPosition();

        try {

//            final LongArrayValues values = value.get();
            final long[] result = new long[1];

            chronicle.bytes().readPosition(offset);
            chronicle.wire.readDocument(wireIn -> {

                wireIn.read(() -> "index").int64array(values, this, (o, v) -> o.values = v);
                result[0] = values.getVolatileValueAt(index);

            }, null);

            return result[0];

        } finally {
            chronicle.bytes().readPosition(pos);
        }

    }


    @Override
    public ChronicleQueue queue() {
        return chronicle;
    }
}


