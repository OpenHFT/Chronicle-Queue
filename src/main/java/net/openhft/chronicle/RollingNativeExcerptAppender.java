package net.openhft.chronicle;

import net.openhft.lang.io.NativeBytes;

import java.io.IOException;

/**
 * @author peter.lawrey
 */
public class RollingNativeExcerptAppender extends NativeBytes implements ExcerptAppender {
    private final RollingChronicle chronicle;
    private long size;

    public RollingNativeExcerptAppender(RollingChronicle chronicle) throws IOException {
        super(0, 0, 0);
        this.chronicle = chronicle;
        size = chronicle.fileCache.findLast();
    }

    @Override
    public Chronicle chronicle() {
        return chronicle;
    }

    public long size() {
        return size;
    }

    @Override
    public void roll() {
        chronicle.fileCache.roll();
    }

    @Override
    public void startExcerpt(int capacity) {
    }

    @Override
    public long index() {
        return size;
    }

    @Override
    public ExcerptAppender toEnd() {
        return this;
    }
}
