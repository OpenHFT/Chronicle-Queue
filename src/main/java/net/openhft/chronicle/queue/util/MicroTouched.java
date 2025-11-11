/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.util;

/**
 * Interface representing small operations that can be used to reduce jitter in threads.
 * <p>Provides methods to perform tiny operations either on the current thread or in a background thread to improve performance consistency.
 */
public interface MicroTouched {
    /**
     * Performs a tiny operation to improve jitter in the current thread.
     * <p>This method should be called in contexts where reducing jitter or improving performance consistency is desired.
     *
     * @return {@code true} if the operation was successful, otherwise {@code false}
     */
    boolean microTouch();

    /**
     * Performs a small operation to improve jitter in a background thread.
     * <p>This method is designed to be executed in a background thread to smooth out performance fluctuations.
     */
    void bgMicroTouch();
}
