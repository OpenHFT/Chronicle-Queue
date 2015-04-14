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
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static net.openhft.chronicle.queue.impl.Indexer.IndexOffset.toAddress0;
import static net.openhft.chronicle.queue.impl.Indexer.IndexOffset.toAddress1;

/**
 * Created by peter.lawrey on 30/01/15.
 */
public class SingleTailer implements ExcerptTailer {
    @NotNull
    private final SingleChronicleQueue chronicle;
    long index;
    private final BytesStoreBytes bytes = new BytesStoreBytes(Bytes.elasticByteBuffer());
    private final Wire wire = new BinaryWire(bytes);

    public SingleTailer(ChronicleQueue chronicle) {
        this.chronicle = (SingleChronicleQueue) chronicle;
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

    @Override
    public boolean index(final long index) {

        long address0 = chronicle.indexToIndex() + toAddress0(index);
        long address1 = chronicle.bytes().readVolatileLong(address0);
        long address2 = 0;
        long start = 0;

        if (address1 != 0) {
            long offset = address1 + toAddress1(index);
            address2 = chronicle.bytes().readVolatileLong(offset);
            if (address2 != 0) {
                wire.bytes().position(address2);
                start = ((index / 64L)) * 64L;
            }
        }

        // scan from the last known index
        if (address2 == 0) {
            long lastKnownIndex = 0;
            long newAddress0 = 0;
            int count = 0;
            for (newAddress0 = chronicle.indexToIndex(); count < ((int) (1L << 17L)); newAddress0 += 8, count++) {

                long l = chronicle.bytes().readVolatileLong(newAddress0);
                if (l != 0) {
                    address1 = l;
                    if (count > 0)
                        lastKnownIndex += (1L << (17L + 6L));
                } else
                    break;
            }

            if (address1 != 0) {
                long newAddress1;
                for (newAddress1 = address1, count = 0; count < ((int) (1L << 17L)); newAddress1 += 8, count++) {

                    long l = chronicle.bytes().readVolatileLong(newAddress1);
                    if (l != 0) {
                        address2 = l;
                        if (count > 0)
                            lastKnownIndex += (1L << (6L));
                    } else
                        break;

                }
            }

            if (address2 != 0) {
                wire.bytes().position(address2);
                start = lastKnownIndex;
            }

        }

        final AtomicLong position = new AtomicLong(-1);
        long last = chronicle.lastIndex();

        // linear scan the last part
        for (long i = start; i < last; i++) {
            final long j = i;

            wire.readDocument(null, wireIn -> {
                if (index == j)
                    position.set(wire.bytes().position() - 4);
            });


            if (position.get() != -1) {
                wire.bytes().position(position.get());
                return true;
            }
        }
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


