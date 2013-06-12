package net.openhft.chronicle;

import net.openhft.lang.io.NativeBytes;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author peter.lawrey
 */
public class NativeExcerptWriter extends NativeBytes implements ExcerptWriter {
    private final IndexedChronicle chronicle;
    @SuppressWarnings("FieldCanBeLocal")
    private MappedByteBuffer indexBuffer, dataBuffer;

    public NativeExcerptWriter(IndexedChronicle chronicle) {
        super(0, 0, 0);
        this.chronicle = chronicle;
    }


    // relatively static
    private long indexStart = -IndexedChronicle.INDEX_BLOCK_SIZE, indexStartAddr, indexLimitAddr;
    private long dataStart = -IndexedChronicle.DATA_BLOCK_SIZE, dataLimitAddr;
    // changed per line
    private long dataPositionAtStartOfLine;
    // changed per entry.
    private long indexPositionAddr;

    public void startExcerpt(int capacity) {
        // check we are the start of a block.
        checkNewIndexLine();

        // if the capacity is to large, roll the previous entry, and there was one
        if (positionAddr + capacity > dataLimitAddr) {
            windToNextDataBuffer();
        }

        // update the soft limitAddr
        limitAddr = positionAddr + capacity;
    }

    private void checkNewIndexLine() {
        if ((indexPositionAddr & (IndexedChronicle.LINE_SIZE - 1)) == 0) {
            newIndexLine();
        }
    }

    private void windToNextDataBuffer() {
        try {
            if (dataLimitAddr != 0)
                padPreviousEntry();
            loadNextDataBuffer();
            checkNewIndexLine();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void padPreviousEntry() {
        positionAddr = dataLimitAddr;
        // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + "= 0xFFFFFFFF");
        UNSAFE.putOrderedInt(null, indexPositionAddr, 0xFFFFFFFF);
        indexPositionAddr += 4;
    }

    private void loadNextDataBuffer() throws IOException {
        dataStart += IndexedChronicle.DATA_BLOCK_SIZE;
        dataBuffer = chronicle.dataFile.map(FileChannel.MapMode.READ_WRITE, dataStart, IndexedChronicle.DATA_BLOCK_SIZE);
        startAddr = positionAddr = ((DirectBuffer) dataBuffer).address();
        dataLimitAddr = startAddr + IndexedChronicle.DATA_BLOCK_SIZE;
    }

    private long dataPosition() {
        return positionAddr - startAddr + dataStart;
    }

    @Override
    public void finish() {
        super.finish();

        // push out the entry is available.  This is what the reader polls.
        // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + "= " + (int) (dataPosition() - dataPositionAtStartOfLine));
        UNSAFE.putOrderedInt(null, indexPositionAddr, (int) (dataPosition() - dataPositionAtStartOfLine));
        indexPositionAddr += 4;
    }

    private void newIndexLine() {
        // check we have a valid index
        if (indexPositionAddr >= indexLimitAddr) {
            try {
                // roll index memory mapping.

                indexStart += IndexedChronicle.INDEX_BLOCK_SIZE;
                indexBuffer = chronicle.indexFile.map(FileChannel.MapMode.READ_WRITE, indexStart, IndexedChronicle.INDEX_BLOCK_SIZE);
                indexStartAddr = indexPositionAddr = ((DirectBuffer) indexBuffer).address();
                indexLimitAddr = indexStartAddr + IndexedChronicle.INDEX_BLOCK_SIZE;
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        // sets the base address
        dataPositionAtStartOfLine = dataPosition();
        UNSAFE.putOrderedLong(null, indexPositionAddr, dataPositionAtStartOfLine);
        // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + "=== " + dataPositionAtStartOfLine);

        indexPositionAddr += 8;
    }
}
