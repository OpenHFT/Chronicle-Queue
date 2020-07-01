package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.ReferenceCounted;
import net.openhft.chronicle.core.io.ReferenceOwner;
import net.openhft.chronicle.core.util.ThrowingFunction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Thread-safe, self-cleaning cache for ReferenceCounted objects
 */
public class ReferenceCountedCache<K, T extends ReferenceCounted & Closeable, V, E extends Throwable>
        extends AbstractCloseable {
    private final Map<K, T> cache = new LinkedHashMap<>();
    private final Function<T, V> transformer;
    private final ThrowingFunction<K, T, E> creator;
    private final ReferenceOwner storeOwner;

    public ReferenceCountedCache(Function<T, V> transformer,
                                 ThrowingFunction<K, T, E> creator,
                                 ReferenceOwner storeOwner) {
        this.transformer = transformer;
        this.creator = creator;
        this.storeOwner = storeOwner;
    }

    @NotNull
    synchronized V get(@NotNull final K key) throws E {

        // remove all which have been dereferenced. Garbagey but rare
        cache.entrySet().removeIf(entry -> entry.getValue().refCount() == 0);

        @Nullable T value = cache.get(key);

        // another thread may have reduced refCount since removeIf above
        if (value == null) {
            // worst case is that 2 threads create at 'same' time
            value = creator.apply(key);
            value.reserveTransfer(INIT, storeOwner);
            cache.put(key, value);
        }

        return transformer.apply(value);
    }

    @Override
    protected synchronized void performClose() {
        cache.forEach((k, v) -> v.release(storeOwner));
    }

    @Override
    protected boolean threadSafetyCheck(boolean isUsed) {
        return true;
    }
}
