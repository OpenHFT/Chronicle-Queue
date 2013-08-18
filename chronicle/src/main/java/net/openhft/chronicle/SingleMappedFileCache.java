package net.openhft.chronicle;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * User: peter
 * Date: 17/08/13
 * Time: 14:58
 */
public class SingleMappedFileCache implements MappedFileCache {
    final String basePath;
    final FileChannel fileChannel;
    final int blockSize;

    public SingleMappedFileCache(String basePath, int blockSize) throws FileNotFoundException {
        this.basePath = basePath;
        this.blockSize = blockSize;
        fileChannel = new RandomAccessFile(basePath, "rw").getChannel();
    }

    @Override
    public MappedByteBuffer acquireBuffer(long index) {
        long start = System.nanoTime();
        MappedByteBuffer mappedByteBuffer;
        try {
            mappedByteBuffer = MapUtils.getMap(fileChannel, index * blockSize, blockSize);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        long time = System.nanoTime() - start;
        if (time > 50e3)
            System.out.println("Took " + time / 1000 + " us to obtain a data chunk");
        return mappedByteBuffer;
    }

    @Override
    public long findLast() throws IOException {
        return fileChannel.size() / blockSize - 1;
    }

    @Override
    public void close() {
        try {
            fileChannel.close();
        } catch (IOException ignored) {
        }
    }
}
