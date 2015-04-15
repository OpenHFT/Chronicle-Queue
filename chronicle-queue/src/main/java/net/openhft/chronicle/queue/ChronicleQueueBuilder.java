package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.SingleChronicleQueue;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * Created by peter.lawrey on 30/01/15.
 */
public class ChronicleQueueBuilder {
    private String name;
    private long blockSize = 64 << 20;
    private Class<? extends Wire> wireType = BinaryWire.class;

    public ChronicleQueueBuilder(String name) {
        this.name = name;
    }

    @NotNull
    public ChronicleQueueBuilder blockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public ChronicleQueueBuilder wireType(Class<? extends Wire> wireType) {
        this.wireType = wireType;
        return this;
    }


    @NotNull
    public SingleChronicleQueue build() throws IOException {
        return new SingleChronicleQueue(name, blockSize, wireType);
    }
}
