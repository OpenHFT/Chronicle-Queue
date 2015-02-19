package net.openhft.chronicle.queue;

import net.openhft.lang.io.MappedFile;
import net.openhft.lang.io.MappedMemory;
import net.openhft.lang.io.MappedNativeBytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

/**
 * Created by Rob Austin
 */
public class MappingReferenceCountTest {


    /**
     * tests that blocks are acquired and released as needed
     *
     * @throws Exception
     */
    @Test
    public void testMappingReferenceCount() throws Exception {

        File tempFile = File.createTempFile("chronicle", "q");

        try {
            int BLOCK_SIZE = 64;
            final MappedFile mappedFile = new MappedFile(tempFile.getName(), BLOCK_SIZE, 0);
            final MappedNativeBytes bytes = new MappedNativeBytes(mappedFile, true);


            // write into block 1
            bytes.writeLong(64 + 8, Long.MAX_VALUE);
            Assert.assertEquals(1, mappedFile.getMap(1).refCount());

            // we move from block 1 to block 2
            bytes.writeLong((64 * 2) + 8, Long.MAX_VALUE);
            Assert.assertEquals(0, mappedFile.getMap(1).refCount());
            Assert.assertEquals(1, mappedFile.getMap(2).refCount());


            // we move from block 2 back to block 1
            bytes.writeLong((64 * 1) + 8, Long.MAX_VALUE);
            Assert.assertEquals(1, mappedFile.getMap(1).refCount());
            Assert.assertEquals(0, mappedFile.getMap(2).refCount());

            // we move from block 2 back to block 1
            bytes.writeLong((64 * 3) + 8, Long.MAX_VALUE);
            Assert.assertEquals(1, mappedFile.getMap(3).refCount());


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            tempFile.delete();
        }
    }
}
