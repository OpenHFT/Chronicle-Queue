package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.bytes.internal.SingleMappedFile;
import net.openhft.chronicle.core.CleaningRandomAccessFile;
import net.openhft.chronicle.core.OS;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Paths;

public class SparseFileTest {

    @Test
    public void test() throws FileNotFoundException {
        File file = new File(Paths.get(OS.getTarget(), "sparseFileTest").toString());
        SingleMappedFile smf = new SingleMappedFile(file, new CleaningRandomAccessFile(file, "rw"), 2*1024*1024, 2*1024*1024, false);
    }

}
