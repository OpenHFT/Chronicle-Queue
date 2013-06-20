package net.openhft.chronicle;

import net.openhft.lang.io.NativeBytes;

/**
 * @author peter.lawrey
 */
public class RollingNativeExcerptTailer extends NativeBytes implements ExcerptTailer {
    private final RollingChronicle chronicle;
    long index = -1;

    public RollingNativeExcerptTailer(RollingChronicle chronicle) {
        super(0, 0, 0);
        this.chronicle = chronicle;
    }

    @Override
    public ExcerptTailer toStart() {
        index = -1;
        return this;
    }

    @Override
    public ExcerptTailer toEnd() {
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
