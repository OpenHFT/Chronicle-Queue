package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.wire.Sequence;

public class SequenceImpl implements Sequence {
    public SequenceImpl(LongValue sequence0, LongValue writePosition) {

    }

    @Override
    public long sequence() {
        return 0;
    }

    @Override
    public void sequence(long sequence, long position) {

    }
}
