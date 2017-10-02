package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.ReferenceCounted;
import net.openhft.chronicle.core.ReferenceCounter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class DirectoryContent implements ReferenceCounted {
    private static final int RECORD_COUNT = 8;
    private final Bytes bytes;
    private final ReferenceCounter rc;

    DirectoryContent(final Path queuePath) {
        final File file = queuePath.resolve("directory-contents.idx").toFile();
        try {
            Files.createDirectories(queuePath);
            file.createNewFile();
            final RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.setLength(RECORD_COUNT * 4);
            raf.close();
            bytes = MappedBytes.mappedBytes(file, 4);
            rc = ReferenceCounter.onReleased(bytes::release);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public int getMostRecentRollCycle()
    {
        int maxRollCycle = 0;
        for (int i = 0; i < RECORD_COUNT; i++) {
            maxRollCycle = Math.max(maxRollCycle, bytes.readVolatileInt(i * 4));
        }

        return maxRollCycle;
    }

    public void onFileCreated(final Path path, final int rollCycle)
    {
        final int index = rollCycle & (RECORD_COUNT - 1);
        while (true) {
            final int currentCycle = bytes.readVolatileInt(index * 4);
            if (rollCycle > currentCycle) {
                if (bytes.compareAndSwapInt(index * 4, currentCycle, rollCycle)) {
                    return;
                }
            } else {
                return;
            }
        }
    }

    public void onFileDeleted(final Path path, final int rollCycle)
    {
        final int index = rollCycle & (RECORD_COUNT - 1);
        while (true) {
            final int currentCycle = bytes.readVolatileInt(index * 4);
            if (rollCycle == currentCycle) {
                if (bytes.compareAndSwapInt(index * 4, rollCycle, 0)) {
                    return;
                }
            } else {
                return;
            }
        }
    }

    @Override
    public void reserve() throws IllegalStateException {
        rc.reserve();
    }

    @Override
    public void release() throws IllegalStateException {
        rc.release();
    }

    @Override
    public long refCount() {
        return rc.get();
    }
}
