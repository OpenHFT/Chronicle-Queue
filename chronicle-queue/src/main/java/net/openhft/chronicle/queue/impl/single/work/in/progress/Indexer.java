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

import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.core.values.LongArrayValues;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.AbstractChronicleQueue;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import static java.lang.ThreadLocal.withInitial;
import static net.openhft.chronicle.queue.impl.single.work.in.progress.Indexer.IndexOffset.toAddress0;
import static net.openhft.chronicle.queue.impl.single.work.in.progress.Indexer.IndexOffset.toAddress1;


/**
 * this class is not threadsafe - first CAS has to be implemented
 *
 * @author Rob Austin.
 */
public class Indexer {

    static final long UNINITIALISED = 0L;

    // 1 << 20 ( is 1MB ), a long is 8 Bytes, if we were to just store the longs in 1Mb this
    // would give use 1 << 17 longs.
    public static final long NUMBER_OF_ENTRIES_IN_EACH_INDEX = 1 << 17;
    private final AbstractChronicleQueue chronicle;
    private ThreadLocal<ByteableLongArrayValues> array;

    public Indexer(@NotNull final ChronicleQueue chronicle, WireType wireType) {
        this.chronicle = (AbstractChronicleQueue) chronicle;
        this.array = newLongArrayValuesPool(wireType);

    }

    public static ThreadLocal<ByteableLongArrayValues>
    newLongArrayValuesPool(WireType wireType) {

        if (wireType == WireType.TEXT)
            return withInitial(TextLongArrayReference::new);
        if (wireType == WireType.BINARY)
            return withInitial(BinaryLongArrayReference::new);
        else
            throw new IllegalStateException("todo, unsupported type=" + wireType);

    }


    /**
     * sans through every excerpts and records every 64th address in the index2indexs'
     *
     * @throws Exception
     */

    public synchronized void index() throws Exception {

        final ExcerptTailer tailer = chronicle.createTailer();

        for (long i = 0; i <= chronicle.lastWrittenIndex(); i++) {

            final long index = i;

            tailer.readDocument(wireIn -> {
                long address = wireIn.bytes().readPosition() - 4;
                recordAddress(index, address);
                wireIn.bytes().readSkip(wireIn.bytes().readRemaining());
            });

        }
    }

    /**
     * records every 64th address in the index2index
     *
     * @param index   the index of the Excerpts which we are going to record
     * @param address the address of the Excerpts which we are going to record
     */
    private void recordAddress(long index, long address) {

        if (index % 64 != 0)
            return;

        final LongArrayValues array = this.array.get();
        final long index2Index = chronicle.indexToIndex();

        chronicle.wire().readDocument(index2Index, rootIndex -> {

            rootIndex.read(() -> "index").int64array(array, this, (o, o2) -> {
            });

            long secondaryAddress = array.getValueAt(toAddress0(index));

            if (secondaryAddress == UNINITIALISED)
                array.setValueAt(index, secondaryAddress = chronicle.newIndex());

            chronicle.wire().readDocument(secondaryAddress, new ReadMarshallable() {

                @Override
                public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
                    wire.read(() -> "index").int64array(array, this, (o, o2) -> {
                    });
                    array.setValueAt(toAddress1(index), address);
                }
            }, null);


        }, null);


    }


    public enum IndexOffset {
        ;

        public static long toAddress0(long index) {

            long siftedIndex = index >> (17L + 6L);
            long mask = (1L << 17L) - 1L;
            long maskedShiftedIndex = mask & siftedIndex;

            // convert to an offset
            return maskedShiftedIndex * 8L;
        }

        public static long toAddress1(long index) {

            long siftedIndex = index >> (6L);
            long mask = (1L << 17L) - 1L;
            long maskedShiftedIndex = mask & siftedIndex;

            // convert to an offset
            return maskedShiftedIndex * 8L;
        }


        @NotNull
        public static String toBinaryString(long i) {

            StringBuilder sb = new StringBuilder();

            for (int n = 63; n >= 0; n--)
                sb.append(((i & (1L << n)) != 0 ? "1" : "0"));

            return sb.toString();
        }

        @NotNull
        public static String toScale() {

            StringBuilder units = new StringBuilder();
            StringBuilder tens = new StringBuilder();

            for (int n = 64; n >= 1; n--)
                units.append((0 == (n % 10)) ? "|" : n % 10);

            for (int n = 64; n >= 1; n--)
                tens.append((0 == (n % 10)) ? n / 10 : " ");

            return units.toString() + "\n" + tens.toString();
        }


    }


}
