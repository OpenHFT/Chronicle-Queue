package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.OS;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class HugetlbfsTest {

    @Test
    public void mappedBytes() throws IOException {
        File file = new File("/mnt/huge/test/testfile");
        file.createNewFile();
        int hugePageSize = 2048 * 1024;
        MappedBytes bytes = MappedBytes.mappedBytes(file, OS.pageSize(), OS.pageSize(), hugePageSize, true);
        int value = bytes.readVolatileInt(0);
    }

}
