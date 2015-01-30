package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.SingleChronicle;

import java.io.IOException;

/**
 * Created by peter.lawrey on 30/01/15.
 */
public class ChronicleQueueBuilder {
    private String name;
    private long blockSize;

    public Chronicle build() throws IOException {
        return new SingleChronicle(name, blockSize);
    }
}
