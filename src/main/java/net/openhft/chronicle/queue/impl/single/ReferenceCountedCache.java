package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.ReferenceCounted;
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
    private final Runnable bgCleanup = this::bgCleanup;

    public ReferenceCountedCache(Function<T, V> transformer,
                                 ThrowingFunction<K, T, E> creator) {
        this.transformer = transformer;
        this.creator = creator;
    }

    @NotNull V get(@NotNull final K key) throws E {

        @Nullable T value;
        synchronized (cache) {
            throwExceptionIfClosed();

            value = cache.get(key);

            if (value != null && value.refCount() == 0)
                value.close();

            // another thread may have reduced refCount since removeIf above
            if (value == null || value.isClosed()) {
                // worst case is that 2 threads create at 'same' time
                value = creator.apply(key);
                value.reserveTransfer(INIT, this);
//                System.err.println("Reserved " + value.toString() + " by " + this);
                cache.put(key, value);
            }
        }
        BackgroundResourceReleaser.run(bgCleanup);

        return transformer.apply(value);
    }

    @Override
    protected void performClose() {
//        System.err.println("performClose on " + this);
        synchronized (cache) {
            for (T value : cache.values()) {
                try {
                    value.release(this);
//                    System.err.println("Released " + value + " by " + this);
                } catch (Exception e) {
                    Jvm.debug().on(getClass(), e);
                }
            }
        }
    }

    @Override
    protected boolean threadSafetyCheck(boolean isUsed) {
        return true;
    }

    void bgCleanup() {
        synchronized (cache) {
            // remove all which have been dereferenced. Garbagey but rare
            cache.entrySet().removeIf(entry -> entry.getValue().refCount() == 0);
        }
    }
}
