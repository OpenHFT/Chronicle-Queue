package run.chronicle.staged;

import net.openhft.chronicle.values.Array;

public interface IFacadeAll extends IFacadeSon {
    @Array(length = 42)
    void setTimestampAt(int i, long v);

    long getTimestampAt(int i);
}
