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
 * Thread-safe, self-cleaning cache for ReferenceCounted (& Closeable) objects
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
        throwExceptionIfClosed();

        final V rv;
        synchronized (cache) {
            @Nullable T value = cache.get(key);

            if (value == null) {
                value = creator.apply(key);
                value.reserveTransfer(INIT, this);
                //System.err.println("Reserved " + value.toString() + " by " + this);
                cache.put(key, value);
            }

            // this will add to the ref count and so needs to be done inside of sync block
            rv = transformer.apply(value);
        }

        BackgroundResourceReleaser.run(bgCleanup);

        return rv;
    }

    @Override
    protected void performClose() {
        bgCleanup();
        if (! cache.isEmpty()) {
            Jvm.warn().on(getClass(), "Cache should have been cleaned");
            synchronized (cache) {
                for (T value : cache.values()) {
                    try {
                        value.release(this);
//                    System.err.println("Released (performClose) " + value + " by " + this);
                    } catch (Exception e) {
                        Jvm.debug().on(getClass(), e);
                    }
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
            // remove all which have been dereferenced by other than me. Garbagey but rare
            cache.entrySet().removeIf(entry -> {
                T value = entry.getValue();
                boolean noOtherReferencers = value.refCount() == 1;
                if (noOtherReferencers) {
                    //System.err.println("Removing " + value.toString() + " by " + this);
                    value.release(this);
                    value.close();
                }
                return noOtherReferencers;
            });
        }
    }
}
