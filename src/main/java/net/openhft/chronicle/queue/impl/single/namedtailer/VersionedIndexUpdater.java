package net.openhft.chronicle.queue.impl.single.namedtailer;

import net.openhft.chronicle.core.values.LongValue;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

public class VersionedIndexUpdater implements IndexUpdater, Closeable {

    private final LongValue indexValue;

    private final LongValue indexVersionValue;

    public VersionedIndexUpdater(@NotNull LongValue indexValue, @NotNull LongValue indexVersionValue) {
        this.indexValue = indexValue;
        this.indexVersionValue = indexVersionValue;
    }

    @Override
    public void close() throws IOException {
        closeQuietly(indexValue, indexVersionValue);
    }

    @Override
    public void update(long index) {
        indexValue.setValue(index);
        indexVersionValue.addAtomicValue(1);
    }

    @Override
    public LongValue index() {
        return indexValue;
    }
}
