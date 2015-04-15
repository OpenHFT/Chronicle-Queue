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

package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStoreBytes;
import net.openhft.chronicle.core.values.LongArrayValues;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.ByteableLongArrayValues;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;
import java.util.function.Function;

import static net.openhft.chronicle.queue.impl.Indexer.IndexOffset.toAddress0;
import static net.openhft.chronicle.queue.impl.Indexer.IndexOffset.toAddress1;
import static net.openhft.chronicle.queue.impl.Indexer.newLongArrayValuesPool;

/**
 * Created by peter.lawrey on 30/01/15.
 */
public class SingleTailer implements ExcerptTailer {
    @NotNull

    private final SingleChronicleQueue chronicle;

    private long index;

    private final BytesStoreBytes bytes = new BytesStoreBytes(Bytes.elasticByteBuffer());

    private final Wire wire;

    private ThreadLocal<ByteableLongArrayValues> value;

    public SingleTailer(@NotNull final AbstractChronicle chronicle,
                        @NotNull final Function<Bytes, Wire> wireProvider) {
        this.chronicle = (SingleChronicleQueue) chronicle;
        this.wire = wireProvider.apply(bytes);
        this.value = newLongArrayValuesPool(chronicle.wireType());
        toStart();
    }

    @Override
    public WireIn wire() {
        return new ChronicleWireIn(null);
    }

    @Override
    public boolean readDocument(Consumer<WireIn> reader) {
        wire.readDocument(null, reader);
        return true;
    }


    /**
     * reads an item in the index, the index is stored in meta data
     *
     * @param offset the address of the document
     * @param index  the index of of the array item
     * @return returns a long at array {@code index}
     */
    private long readIndexAt(long offset, long index) {

        if (offset == 0)
            return 0;

        long pos = chronicle.bytes().position();

        try {

            final LongArrayValues values = value.get();
            final long[] result = new long[1];

            chronicle.bytes().position(offset);
            chronicle.wire.readDocument(wireIn -> {

                wireIn.read(() -> "index").int64array(values, null);
                result[0] = values.getVolatileValueAt(index);

            }, null);

            return result[0];

        } finally {
            chronicle.bytes().position(pos);
        }

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
        long pos = bytes.position();
        long start = 0;

        long address = 0;
        long index2index = chronicle.indexToIndex();
        assert index2index != 0;

        long address1 = readIndexAt(index2index, toAddress0(index));
        long address2;

        if (address1 != 0) {

            address2 = readIndexAt(address1, toAddress1(index));

            if (address2 != 0) {
                wire.bytes().position(address2);
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
            bytes.position(address);
        else
            // start to read just after the header
            bytes.position(Header.PADDED_SIZE);

        bytes.limit(writeByte);
        long last = chronicle.lastIndex();

        // linear scan the last part
        for (long i = start; i < last; i++) {
            final long j = i;

            if (index == j)
                return true;

            readDocument(wireIn -> {
            });

        }

        wire.bytes().position(pos);
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

    @NotNull
    @Override
    public ChronicleQueue chronicle() {
        return chronicle;
    }
}


