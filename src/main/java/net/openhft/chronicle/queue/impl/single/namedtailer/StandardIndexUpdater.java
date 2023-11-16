package net.openhft.chronicle.queue.impl.single.namedtailer;

import net.openhft.chronicle.core.values.LongValue;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

public class StandardIndexUpdater implements IndexUpdater, Closeable {

    private final LongValue indexValue;

    public StandardIndexUpdater(@NotNull LongValue indexValue) {
        this.indexValue = indexValue;
    }

    @Override
    public void close() throws IOException {
        closeQuietly(indexValue);
    }

    @Override
    public void update(long index) {
        indexValue.setValue(index);
    }

    @Override
    public LongValue index() {
        return indexValue;
    }

}
