package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.SingleChronicleQueue;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * Created by peter.lawrey on 30/01/15.
 */
public class ChronicleQueueBuilder {
    private String name;
    private long blockSize = 64 << 20;

    public ChronicleQueueBuilder(String name) {
        this.name = name;
    }

    @NotNull
    public ChronicleQueueBuilder blockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    @NotNull
    public ChronicleQueue build() throws IOException {
        return new SingleChronicleQueue(name, blockSize);
    }
}
