package net.openhft.chronicle.queue.util;

public interface MicroTouched {
    /**
     * perform a tiny operation to improve jitter in the current thread.
     */
    boolean microTouch();

    /**
     * perofmr a small operation to improve jitter in a background thread.
     */
    void bgMicroTouch();
}
