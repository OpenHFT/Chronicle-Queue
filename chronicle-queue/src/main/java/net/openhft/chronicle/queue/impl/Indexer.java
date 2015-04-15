package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import static java.lang.ThreadLocal.withInitial;
import static net.openhft.chronicle.queue.impl.Indexer.IndexOffset.toAddress1;
import static net.openhft.chronicle.queue.impl.SingleChronicleQueue.UNINITIALISED;

/**
 * this class is not threadsafe - first CAS has to be implemented
 *
 * @author Rob Austin.
 */
public class Indexer {

    // 1 << 20 ( is 1MB ), a long is 8 Bytes, if we were to just store the longs in 1Mb this
    // would give use 1 << 17 longs.
    public static final long NUMBER_OF_ENTRIES_IN_EACH_INDEX = 1 << 17;
    private final AbstractChronicle chronicle;
    private ThreadLocal<ByteableLongArrayValues> array;

    public Indexer(@NotNull final AbstractChronicle chronicle) {
        this.array = newLongArrayValuesPool(chronicle.wireType());
        this.chronicle = chronicle;
    }

    public static ThreadLocal<ByteableLongArrayValues>
    newLongArrayValuesPool(Class<? extends Wire> wireType) {

        if (TextWire.class.isAssignableFrom(wireType))
            return withInitial(LongArrayTextReference::new);
        if (BinaryWire.class.isAssignableFrom(wireType))
            return withInitial(LongArrayDirectReference::new);
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

        for (long i = 0; i <= chronicle.lastIndex(); i++) {

            final long index = i;

            tailer.readDocument(wireIn -> {
                long address = wireIn.bytes().position() - 4;
                recordAddress(index, address);
                wireIn.bytes().skip(wireIn.bytes().remaining());
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

        final ByteableLongArrayValues array = this.array.get();
        final long index2Index = chronicle.indexToIndex();

        chronicle.wire().readDocument(index2Index, rootIndex -> {

            rootIndex.read(() -> "index").int64array(array, null);

            long index0 = IndexOffset.toAddress0(index);
            long secondaryAddress = array.getValueAt(index0);

            if (secondaryAddress == UNINITIALISED) {
                secondaryAddress = chronicle.newIndex();
                array.setValueAt(index, secondaryAddress);
            }

            chronicle.wire().readDocument(secondaryAddress, secondaryIndex -> {

                secondaryIndex.read(() -> "index").int64array(array, null);
                array.setValueAt(toAddress1(index), address);

            }, null);

        }, null);


    }


    public enum IndexOffset {
        ;

        static long toAddress0(long index) {

            long siftedIndex = index >> (17L + 6L);
            long mask = (1L << 17L) - 1L;
            long maskedShiftedIndex = mask & siftedIndex;

            // convert to an offset
            return maskedShiftedIndex * 8L;
        }

        static long toAddress1(long index) {

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
