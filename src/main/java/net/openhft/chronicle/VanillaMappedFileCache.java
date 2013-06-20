package net.openhft.chronicle;

import java.io.IOException;
import java.nio.MappedByteBuffer;

/**
 * @author peter.lawrey
 */
public class VanillaMappedFileCache extends AbstractMappedFileCache {
    public VanillaMappedFileCache(String dirPath, ChronicleConfig config) throws IOException {
        super(dirPath, config);
    }

    @Override
    public MappedByteBuffer acquireMappedBufferForIndex(int indexNumber) throws IOException {
        throw new IOException();
    }

    @Override
    public void randomAccess(boolean randomAccess) {
    }

    @Override
    public void close() {
    }

    @Override
    public void roll() {
    }
}
