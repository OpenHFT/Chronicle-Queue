package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.io.ReferenceChangeListener;
import net.openhft.chronicle.core.io.ReferenceCounted;
import net.openhft.chronicle.core.io.ReferenceOwner;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public enum RCCHolder implements ReferenceChangeListener {

    INSTANCE;

    private final Map<SingleChronicleQueue.StoreSupplier.RCCKey, ReferenceCountedCache<File, MappedFile, MappedBytes, IOException>> map = new ConcurrentHashMap<>();

    public ReferenceCountedCache<File, MappedFile, MappedBytes, IOException> instance(SingleChronicleQueue.StoreSupplier storeSupplier) {
        return map.computeIfAbsent(storeSupplier.rccKey(),
                rccKey -> {
                    final ReferenceCountedCache<File, MappedFile, MappedBytes, IOException> rcc =
                            new ReferenceCountedCache<>(MappedBytes::mappedBytes, storeSupplier::mappedFile);
                    rcc.addReferenceChangeListener(this);
                    return rcc;
                });
    }

    @Override
    public void onReferenceRemoved(@Nullable ReferenceCounted referenceCounted, ReferenceOwner referenceOwner) {
        if (referenceCounted.refCount() == 1) {
            final SingleChronicleQueue.StoreSupplier storeSupplier = (SingleChronicleQueue.StoreSupplier) referenceOwner;
            if (map.remove(storeSupplier.rccKey()) != null) {
                referenceCounted.releaseLast();
            }
        }
    }
}
