package net.openhft.chronicle;

import net.openhft.lang.io.NativeBytes;

/**
 * @author peter.lawrey
 */
public class RollingNativeExcerptReader extends NativeBytes implements ExcerptReader {
    private final RollingChronicle chronicle;
    long index = -1;

    public RollingNativeExcerptReader(RollingChronicle chronicle) {
        super(0, 0, 0);
        this.chronicle = chronicle;
    }

    @Override
    public ExcerptReader toStart() {
        index = -1;
        return this;
    }

    @Override
    public ExcerptReader toEnd() {
        index = chronicle.size();
        return this;
    }

    @Override
    public boolean nextIndex() {
        return false;
    }

    @Override
    public boolean index(long l) {
        return false;
    }

    @Override
    public long index() {
        return index;
    }

    @Override
    public long size() {
        return chronicle.size();
    }

    @Override
    public Chronicle chronicle() {
        return chronicle;
    }
}
