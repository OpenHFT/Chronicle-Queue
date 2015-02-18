package simple;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.MappedFile;
import net.openhft.lang.io.MappedMemory;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.LongValue;

import java.io.File;
import java.io.IOException;

/**
 * Created by peter.lawrey on 02/02/15.
 */
public class WriterMain {
    static final int CHUNK_BITS = 26;

    public static void main(String[] args) throws IOException {
        int dataSize = 256 << 20;
        int length = 50;
        byte[] bytes = new byte[length];
        for (int t = 0; t < 5; t++) {
            long start = System.nanoTime();
            File tmpFile = new File("deleteme" + System.nanoTime() + ".q"); //File.createTempFile("deleteme", "q");
            tmpFile.deleteOnExit();
            MappedFile file = new MappedFile(tmpFile.getName(), 1 << CHUNK_BITS, 1 << CHUNK_BITS);
            FileWriter fw = new FileWriter(file);
            int count = 0;
            for (int i = 0; i < dataSize - 128; i += length + 4) {
                fw.writeData(bytes);
                count++;
            }
            file.close();
            tmpFile.delete();
            long time = System.nanoTime() - start;
            System.out.printf("%,d in %.3f secs, throughput %.6f M/s x %,d%n", count, time / 1e9, count * 1e3 / time, length);
        }
    }

    static class FileWriter {
        static final int NOT_READY = 0x80000000;
        final MappedFile file;
        final LongValue readOffset = DataValueClasses.newDirectReference(LongValue.class);
        long writeOffset;
        private int index = -1;
        private MappedMemory mm;
        private Bytes bytes;

        FileWriter(MappedFile file) throws IOException {
            this.file = file;
            mm = file.acquire(0);
            bytes = mm.bytes();
            ((Byteable) readOffset).bytes(bytes, 0L);
            readOffset.compareAndSwapValue(0, 64);
            writeOffset = readOffset.getVolatileValue();
        }

        public void writeData(byte[] byteArray) throws IOException {
            int index2 = (int) (writeOffset >> CHUNK_BITS);
            if (index2 != index) {
                mm = file.acquire(index2);
                bytes = mm.bytes();
                index = index2;
            }
            int offset2 = (int) (writeOffset & ((1 << CHUNK_BITS) - 1));

            // reserve space
            int length = byteArray.length + 4;
            while (!bytes.compareAndSwapInt(offset2, 0, NOT_READY | length)) {
                offset2 += bytes.readVolatileInt(offset2);
            }
            bytes.write(offset2 + 4, byteArray);
            bytes.writeOrderedInt(offset2, length);
            writeOffset += length;
        }
    }
}
