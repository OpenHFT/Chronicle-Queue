package net.openhft.chronicle.queue.impl.ringbuffer;

import junit.framework.TestCase;
import org.junit.Assert;

public class IndexTest extends TestCase {

    public void testFindExcerpt() throws Exception {
        Index index = new Index();

        Assert.assertEquals(1 * 8, index.secondTranchOffSet(64));
        Assert.assertEquals(1 * 8, index.secondTranchOffSet(65));
        Assert.assertEquals(2 * 8, index.secondTranchOffSet(128));
        Assert.assertEquals(2 * 8, index.secondTranchOffSet(129));
        Assert.assertEquals(8 + (2 * 8), index.secondTranchOffSet(128 + 64));

    }


    public String toScale() {

        StringBuilder units = new StringBuilder();
        StringBuilder tens = new StringBuilder();

        for (int n = 64; n >= 1; n--)
            units.append((0 == (n % 10)) ? "|" : n % 10);

        for (int n = 64; n >= 1; n--)
            tens.append((0 == (n % 10)) ? n / 10 : " ");

        return units.toString() + "\n" + tens.toString();
    }

    public String toBinaryString(long i) {

        StringBuilder sb = new StringBuilder();

        for (int n = 63; n >= 0; n--)
            sb.append(((i & (1L << n)) != 0 ? "1" : "0"));

        return sb.toString();
    }

    public static void main(String[] args) {
        // 2^6 = 64

    }
}