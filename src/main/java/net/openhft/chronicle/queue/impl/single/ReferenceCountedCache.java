package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.*;
import net.openhft.chronicle.core.util.ThrowingFunction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Thread-safe, self-cleaning cache for ReferenceCounted (and Closeable) objects
 */
public class ReferenceCountedCache<K, T extends ReferenceCounted & Closeable, V, E extends Throwable>
        extends AbstractCloseable {

    private final Map<K, T> cache = new LinkedHashMap<>();
    private final Function<T, V> transformer;
    private final ThrowingFunction<K, T, E> creator;
    private final Runnable bgCleanup = this::bgCleanup;

    public ReferenceCountedCache(final Function<T, V> transformer,
                                 final ThrowingFunction<K, T, E> creator) {
        this.transformer = transformer;
        this.creator = creator;

        disableThreadSafetyCheck(true);
    }

    @NotNull
    V get(@NotNull final K key) throws E {
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
        List<T> retained = new ArrayList<>();
        synchronized (cache) {
            for (T value : cache.values()) {
                try {
                    value.release(this);
                    if (value.refCount() > 0)
                        retained.add(value);
//                    System.err.println("Released (performClose) " + value + " by " + this);
                } catch (Exception e) {
                    Jvm.debug().on(getClass(), e);
                }
            }
            cache.clear();
        }
        if (retained.isEmpty() || !Jvm.isResourceTracing())
            return;
        for (int i = 1; i <= 2_500; i++) {
            Jvm.pause(1);
            if (retained.stream().noneMatch(v -> v.refCount() > 0)) {
                if (i > 1)
                    Jvm.perf().on(getClass(), "Took " + i + " to release " + retained);
                return;
            }
        }
        retained.stream()
                .filter(o -> o instanceof ManagedCloseable)
                .map(o -> (ManagedCloseable) o)
                .forEach(ManagedCloseable::warnAndCloseIfNotClosed);
    }

    void bgCleanup() {
        // remove all which have been de-referenced by other than me. Garbagy but rare
        synchronized (cache) {
            cache.entrySet().removeIf(entry -> {
                T value = entry.getValue();
                int refCount = value.refCount();
                if (refCount == 1) {
                    value.release(this);
                }
                return refCount <= 1;
            });
        }
    }
}
