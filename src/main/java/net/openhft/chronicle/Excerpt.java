package net.openhft.chronicle;

import net.openhft.lang.io.Bytes;

/**
 * @author peter.lawrey
 */
public interface Excerpt extends Bytes {
    public long index();

    public long size();

    public Excerpt toEnd();

    public Chronicle chronicle();

    public void finish();
}
