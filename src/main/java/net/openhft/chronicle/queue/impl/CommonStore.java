package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;

public interface CommonStore extends Demarshallable, WriteMarshallable {
    /**
     * @return the file associated with this store.
     */
    @Nullable
    File file();

    @NotNull
    MappedBytes bytes();

    @NotNull
    String dump();

    @NotNull
    String shortDump();

}
