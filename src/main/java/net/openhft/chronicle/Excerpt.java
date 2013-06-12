package net.openhft.chronicle;

import net.openhft.lang.io.Bytes;

/**
 * @author peter.lawrey
 */
public interface Excerpt extends Bytes {
    // index releated
    public long index();

    public boolean nextIndex();

    public boolean index(long l);

    public Excerpt toStart();

    public Excerpt toEnd();

    // others.
    public boolean isPadding(long l);

    public Chronicle chronicle();

    public void finish();
}
