package net.openhft.chronicle;

import java.io.IOException;

/**
 * @author peter.lawrey
 */
public interface MappedFileCache {
    public void randomAccess(boolean randomAccess);

    public long findLast() throws IOException;

    public void close();

    public void roll();
}
