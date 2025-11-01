/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.*;
import net.openhft.chronicle.core.util.ThrowingFunction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * A thread-safe, self-cleaning cache for managing {@link ReferenceCounted} and {@link Closeable} objects.
 * <p>
 * This cache ensures that cached objects are reference-counted, and once the reference count
 * drops to one (held by this cache), the object will be automatically released.
 * The cache performs background cleanup to release resources when the last external reference is removed.
 *
 *
 * @param <K> The type of the cache key
 * @param <T> The type of the cached objects, which must implement both {@link ReferenceCounted} and {@link Closeable}
 * @param <V> The type returned by the transformer function
 * @param <E> The type of any exception that might be thrown during object creation
 */
public class ReferenceCountedCache<K, T extends ReferenceCounted & Closeable, V, E extends Throwable>
        extends AbstractCloseable {

    // Cache to store objects against keys
    private final Map<K, T> cache = new LinkedHashMap<>();
    // Function to transform a cached object into the desired return type
    private final Function<T, V> transformer;
    // Function to create a new object when it's not present in the cache
    private final ThrowingFunction<K, T, E> creator;
    // Listener to handle reference count changes for objects
    private final ReferenceChangeListener referenceChangeListener;

    /**
     * Constructs a {@code ReferenceCountedCache} instance.
     *
     * @param transformer A function to transform a cached object into the return type.
     * @param creator     A function that creates a new object if it is not present in the cache.
     */
    @SuppressWarnings("this-escape")
    public ReferenceCountedCache(final Function<T, V> transformer,
                                 final ThrowingFunction<K, T, E> creator) {
        this.transformer = transformer;
        this.creator = creator;
        this.referenceChangeListener = new TriggerFlushOnLastReferenceRemoval();

        singleThreadedCheckDisabled(true);
    }

    /**
     * Retrieves a cached object or creates a new one if not present.
     * If the object is created, it will be automatically added to the cache.
     *
     * @param key The key to identify the object in the cache.
     * @return The transformed value from the cached object.
     * @throws E If the object creation process encounters an error.
     */
    @NotNull
    V get(@NotNull final K key) throws E {
        throwExceptionIfClosed();

        final V rv;
        synchronized (cache) {
            @Nullable T value = cache.get(key);

            if (value == null) {
                value = creator.apply(key);
                value.reserveTransfer(INIT, this);
                value.addReferenceChangeListener(referenceChangeListener);
                //System.err.println("Reserved " + value.toString() + " by " + this);
                cache.put(key, value);
            }

            // this will add to the ref count and so needs to be done inside of sync block
            rv = transformer.apply(value);
        }

        return rv;
    }

    /**
     * Closes the cache and releases all cached objects.
     * This method ensures all resources held by the cached objects are released.
     */
    @Override
    protected void performClose() {
        synchronized (cache) {
            for (T value : cache.values()) {
                releaseResource(value);
            }
            cache.clear();
        }
    }

    /**
     * Releases the resources of a cached object.
     *
     * @param value The cached object to release.
     */
    private void releaseResource(T value) {
        try {
            if (value != null)
                value.release(this);
        } catch (Exception e) {
            Jvm.debug().on(getClass(), e);
        }
    }

    /**
     * Removes the object associated with the given key from the cache and releases its resources.
     *
     * @param key The key of the object to remove.
     */
    public void remove(K key) {
        // harmless to call if cache is already closing/closed

        synchronized (cache) {
            releaseResource(cache.remove(key));
        }
    }

    /**
     * A listener to trigger cache cleanup when the last reference (besides this cache) is removed.
     */
    private class TriggerFlushOnLastReferenceRemoval implements ReferenceChangeListener {

        private final Runnable bgCleanup = this::bgCleanup;

        @Override
        public void onReferenceRemoved(ReferenceCounted referenceCounted, ReferenceOwner referenceOwner) {
            // If only this cache holds the reference, trigger background cleanup
            if (referenceOwner != ReferenceCountedCache.this && referenceCounted.refCount() == 1) {
                BackgroundResourceReleaser.run(bgCleanup);
            }
        }

        /**
         * Performs background cleanup to remove objects with no remaining references from the cache.
         */
        private void bgCleanup() {
            // remove all which have been de-referenced by other than me. Garbagy but rare
            synchronized (cache) {
                cache.entrySet().removeIf(entry -> {
                    T value = entry.getValue();
                    int refCount = value.refCount();
                    if (refCount == 1) {
                        value.release(ReferenceCountedCache.this);
                    }
                    return refCount <= 1;
                });
            }
        }
    }
}
