package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.WireKey;
import org.jetbrains.annotations.Nullable;

public enum MetaDataField implements WireKey {
    wireType,
    writePosition,
    roll,
    indexing,
    lastAcknowledgedIndexReplicated,
    recovery,
    deltaCheckpointInterval,
    encodedSequence,
    lastIndexReplicated,
    sourceId;

    @Nullable
    @Override
    public Object defaultValue() {
        throw new IORuntimeException("field " + name() + " required");
    }
}
