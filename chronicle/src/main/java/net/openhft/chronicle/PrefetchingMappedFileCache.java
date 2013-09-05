package net.openhft.chronicle;

import net.openhft.lang.thread.NamedThreadFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * User: peter
 * Date: 17/08/13
 * Time: 14:58
 */
public class PrefetchingMappedFileCache implements MappedFileCache {
    public static final AtomicLong totalWait = new AtomicLong();
    static final ExecutorService PREFETCHER = Executors.newCachedThreadPool(new NamedThreadFactory("mmap-prefetch", true));
    private static final IndexedMBB NULL_IMBB = new IndexedMBB(Long.MIN_VALUE, null, -1);
    final String basePath;
    final FileChannel fileChannel;
    final int blockSize;
    long lastIndex = Long.MIN_VALUE;
    MappedByteBuffer lastMBB = null;
    volatile IndexedMBB imbb = NULL_IMBB;

    public PrefetchingMappedFileCache(String basePath, int blockSize) throws FileNotFoundException {
        this.basePath = basePath;
        this.blockSize = blockSize;
        fileChannel = new RandomAccessFile(basePath, "rw").getChannel();
    }

    @Override
    public MappedByteBuffer acquireBuffer(long index) {
        if (index == lastIndex)
            return lastMBB;
//        TreeMap<Long, String> timeMap = new TreeMap<Long, String>();
        long start = System.nanoTime();
//        timeMap.put(start / 1024, "start");
        MappedByteBuffer mappedByteBuffer;

        IndexedMBB indexedMBB = imbb;
        long index0 = indexedMBB.index;
        boolean prefetched = index0 == index;
        try {
            if (prefetched) {
                long waiting = System.nanoTime();
                MappedByteBuffer buffer1;
                while ((buffer1 = indexedMBB.buffer) == null) {
                    Throwable thrown1 = indexedMBB.thrown;
                    if (thrown1 != null) {
                        throw new IllegalStateException(thrown1);
                    }
                }
//                timeMap.put(waiting / 1024, "waiting");
//                timeMap.put(indexedMBB.created / 1024, "created");
//                timeMap.put(indexedMBB.started / 1024, "startMap");
//                timeMap.put(indexedMBB.finished / 1024, "finishMap");
                mappedByteBuffer = buffer1;
            } else
                mappedByteBuffer = MapUtils.getMap(fileChannel, index * blockSize, blockSize);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
//        timeMap.put(System.nanoTime() / 1024, "got");
        lastIndex = index;
        lastMBB = mappedByteBuffer;
        IndexedMBB imbb2 = new IndexedMBB(index + 1, fileChannel, blockSize);
        this.imbb = imbb2;
        PREFETCHER.submit(imbb2);
        long end = System.nanoTime();
        long time = (end - start);
//        timeMap.put(end / 1024, "end");
        if (index > 0)
            totalWait.addAndGet(time);
        if (prefetched) {
//            System.out.println(indexedMBB.report());
        }
//        System.out.println("Took " + time / 1000 + " us to obtain a data chunk, prefetched: " + prefetched + " index0: " + index0 + " index: " + index);
/*
        if (time > 50e3) {
            long first = timeMap.firstKey();
            String sep = "";
            for (Map.Entry<Long, String> entry : timeMap.entrySet()) {
                System.out.print(sep);
                System.out.print(entry.getValue());
                System.out.print(": ");
                System.out.print(entry.getKey() - first);
                sep = ", ";
            }
            System.out.println();
        }
*/
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

    static class IndexedMBB implements Runnable {
        volatile long created, started, finished;
        long index;
        volatile MappedByteBuffer buffer;
        volatile Throwable thrown;
        private FileChannel fileChannel;
        private int blockSize;

        public IndexedMBB(long index, FileChannel fileChannel, int blockSize) {
            created = System.nanoTime();

            this.index = index;
            this.fileChannel = fileChannel;
            this.blockSize = blockSize;
        }

        @Override
        public void run() {
            try {
                started = System.nanoTime();
                buffer = MapUtils.getMap(fileChannel, index * blockSize, blockSize);
                finished = System.nanoTime();
            } catch (Throwable t) {
                thrown = t;
            }
        }

        public String report() {
            return "started: " + (started - created) / 1000 + ", finished: " + (finished - started) / 1000 + ", pick up: " + (System.nanoTime() - finished) / 1000;
        }
    }
}
