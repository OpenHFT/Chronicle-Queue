package net.openhft.chronicle.queue.impl.ringbuffer;

/**
 * @author Rob Austin.
 */
public class Index {

    void findExcerpt(long index) {
        // bottom 7 bit - linuar scan
        // from 8 <--> 25
        // from 26 <--> 42
    }


    long secondTranchOffSet(long index) {

        // remove the unwanted hi bits
        long v = ((1 << 24) - 1) & index;

        // removes the unwanted low bits by shifting off
        long v2 = v >> 6;

        // convert to an offset
        return v2 * 8;
    }

    long thirdTranchOffSet(long index) {


        return 0;
    }
}
