package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.SingleChronicle;

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

    public ChronicleQueueBuilder blockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public Chronicle build() throws IOException {
        return new SingleChronicle(name, blockSize);
    }
}
