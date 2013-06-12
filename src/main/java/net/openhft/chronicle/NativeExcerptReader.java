package net.openhft.chronicle;

import net.openhft.lang.io.NativeBytes;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author peter.lawrey
 */
public class NativeExcerptReader extends NativeBytes implements ExcerptReader {
    private final IndexedChronicle chronicle;
    @SuppressWarnings("FieldCanBeLocal")
    private MappedByteBuffer indexBuffer, dataBuffer;

    public NativeExcerptReader(IndexedChronicle chronicle) {
        super(0, 0, 0);
        this.chronicle = chronicle;
        newIndexLine();
    }

    // relatively static
    private long indexStart = -IndexedChronicle.INDEX_BLOCK_SIZE, indexStartAddr, indexLimitAddr;
    private long dataStart = -IndexedChronicle.DATA_BLOCK_SIZE;
    // changed per line
    private long dataPositionAtStartOfLine;
    // changed per entry.
    private long indexPositionAddr;

    private void loadNextDataBuffer() throws IOException {
        dataStart += IndexedChronicle.DATA_BLOCK_SIZE;
        dataBuffer = getMap(chronicle.dataFile, dataStart, IndexedChronicle.DATA_BLOCK_SIZE);
        startAddr = positionAddr = ((DirectBuffer) dataBuffer).address();
    }

    private MappedByteBuffer getMap(FileChannel fileChannel, long start, int size) throws IOException {
        for (int i = 1; ; i++) {
            try {
                MappedByteBuffer map = fileChannel.map(FileChannel.MapMode.READ_WRITE, start, size);
                if (i > 1)
                    System.out.println("i=" + i);
                return map;
            } catch (IOException e) {
                if (e.getMessage() == null || !e.getMessage().endsWith("user-mapped section open")) {
                    throw e;
                }
                if (i < 10)
                    Thread.yield();
                else
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
            }
        }
    }

    public boolean nextIndex() {
        // update the soft limitAddr
        long offset = UNSAFE.getIntVolatile(null, indexPositionAddr) & 0xFFFFFFFFL;
        // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + " was " + offset);
        if (offset == 0) {
            return false;
        }

        return nextIndex0(offset);
    }

    private boolean nextIndex0(long offset) {
        try {
            checkNewIndexLine2();
            if (offset == 0xFFFFFFFFL) {
                indexPositionAddr += 4;
                loadNextDataBuffer();
                checkNewIndexLine1();
                checkNewIndexLine2();
                return false;
            }
            if (dataPositionAtStartOfLine + offset >= dataStart + IndexedChronicle.DATA_BLOCK_SIZE)
                loadNextDataBuffer();
            limitAddr = (dataPositionAtStartOfLine + offset - dataStart) + startAddr;
            indexPositionAddr += 4;
            return true;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void finish() {
        super.finish();
        // check we are the start of a block.
        checkNewIndexLine1();
        positionAddr = limitAddr;
    }

    private void checkNewIndexLine1() {
        if ((indexPositionAddr & (IndexedChronicle.LINE_SIZE - 1)) == 0) {
            newIndexLine();
        }
    }

    private void checkNewIndexLine2() {
        if ((indexPositionAddr & (IndexedChronicle.LINE_SIZE - 1)) == 8) {
            dataPositionAtStartOfLine = UNSAFE.getLongVolatile(null, indexPositionAddr - 8);
            // System.out.println(Long.toHexString(indexPositionAddr - 8 - indexStartAddr + indexStart) + " WAS " + dataPositionAtStartOfLine);
        }
    }

    private void newIndexLine() {
        // check we have a valid index
        if (indexPositionAddr >= indexLimitAddr) {
            try {
                // roll index memory mapping.

                indexStart += IndexedChronicle.INDEX_BLOCK_SIZE;
                indexBuffer = getMap(chronicle.indexFile, indexStart, IndexedChronicle.INDEX_BLOCK_SIZE);
                indexStartAddr = indexPositionAddr = ((DirectBuffer) indexBuffer).address();
                indexLimitAddr = indexStartAddr + IndexedChronicle.INDEX_BLOCK_SIZE;
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        indexPositionAddr += 8;
    }
}
