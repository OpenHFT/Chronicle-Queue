package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.queue.impl.SingleChronicleQueue.BUILDING;
import static net.openhft.chronicle.queue.impl.SingleChronicleQueue.UNINITIALISED;

/**
 * @author Rob Austin.
 */
public class Indexer {

    /**
     * sans through every excerpts and records every 64th address in the index2indexs'
     *
     * @param chronicle
     * @throws Exception
     */
    public static void index(@NotNull final ChronicleQueue chronicle) throws Exception {

        final SingleChronicleQueue single = (SingleChronicleQueue) chronicle;

        final long index2Index = single.indexToIndex();

        final ExcerptTailer tailer = chronicle.createTailer();
        System.out.println(" single.lastIndex()=" + single.lastIndex());
        for (long i = 0; i <= single.lastIndex(); i++) {

            final long index = i;


            tailer.readDocument(wireIn -> {
                long address = wireIn.bytes().position() - 4;
                System.out.println("index=" + index);
                System.out.println("index2Index=" + index2Index);
                System.out.println("address=" + address);

                recordAddress(index, address, single, index2Index);
                wireIn.bytes().skip(wireIn.bytes().remaining());
            });

        }
    }

    /**
     * records every 64th address in the index2index
     *
     * @param index          the index of the Excerpts
     * @param address        the address of the Excerpts
     * @param chronicleQueue the chronicle queue
     * @param index2Index    the index2index address
     */
    private static void recordAddress(long index,
                                      long address,
                                      @NotNull final SingleChronicleQueue chronicleQueue,
                                      final long index2Index) {

        if (index % 64 != 0)
            return;

        long offset = IndexOffset.toAddress0(index);
        Bytes chronicleBytes = chronicleQueue.bytes();
        long rootOffset = index2Index + offset;

        long refToSecondary = chronicleBytes.readVolatileLong(rootOffset);

        if (refToSecondary == UNINITIALISED) {
            boolean success = chronicleBytes.compareAndSwapLong(rootOffset, UNINITIALISED, BUILDING);
            if (!success) {
                refToSecondary = chronicleBytes.readVolatileLong(rootOffset);
            } else {
                refToSecondary = chronicleQueue.newIndex();

                chronicleBytes.writeOrderedLong(rootOffset, refToSecondary);
            }
        }

        long l = chronicleBytes.bytes().readLong(refToSecondary + IndexOffset.toAddress1(index));
        assert l == UNINITIALISED;

        long offset1 = refToSecondary + IndexOffset.toAddress1(index);
        chronicleBytes.bytes().writeOrderedLong(offset1, address);


    }

    public  enum IndexOffset {
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
        public  static String toScale() {

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
