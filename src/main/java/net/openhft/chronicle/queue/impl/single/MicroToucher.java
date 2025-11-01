/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.wire.Wire;
import net.openhft.posix.MSyncFlag;
import net.openhft.posix.PosixAPI;

/**
 * The {@code MicroToucher} class is responsible for managing memory page touches
 * and syncing to ensure that memory writes are persisted efficiently.
 * It is used in conjunction with the {@link StoreAppender} to track and handle
 * the memory pages that need touching or syncing during append operations.
 */
public class MicroToucher {
    private final StoreAppender appender;
    private long lastPageTouched = 0;
    private volatile long lastPageToSync = 0;
    private long lastPageSynced = 0;

    /**
     * Constructs a {@code MicroToucher} with a specified {@link StoreAppender}.
     *
     * @param appender The appender associated with this micro-toucher, used to
     *                 retrieve the wire and manage positions for memory touching.
     */
    public MicroToucher(StoreAppender appender) {
        this.appender = appender;
    }

    /**
     * Executes the page-touching logic. It calculates the memory page where the appender's last
     * position is and attempts to "touch" the next page. This ensures that the memory pages
     * are kept in an active state as data is written.
     *
     * @return {@code true} if the page was successfully touched, {@code false} otherwise.
     */
    public boolean execute() {
        final Wire bufferWire = appender.wire();
        if (bufferWire == null)
            return false;

        final long lastPosition = appender.lastPosition;
        final long lastPage = lastPosition & ~0xFFF;
        final long nextPage = (lastPosition + 0xFFF) & ~0xFFF;
        Bytes<?> bytes = bufferWire.bytes();
        if (nextPage != lastPageTouched) {
            lastPageTouched = nextPage;
            try {
                // best effort
                final BytesStore<?, ?> bs = bytes.bytesStore();
                if (bs.inside(nextPage, 8))
                    touchPage(nextPage, bs);
            } catch (Throwable ignored) {
            }
            return true;
        }

        lastPageToSync = lastPage;
        return false;
    }

    /**
     * Executes the background sync operation, syncing memory pages to disk.
     * It ensures that pages touched earlier are asynchronously written to disk using
     * the {@link PosixAPI#msync} function.
     */
    public void bgExecute() {
        final long lastPage = this.lastPageToSync;
        final long start = this.lastPageSynced;
        final long length = Math.min(8 << 20, lastPage - start);
//        System.out.println("len "+length);
        if (length < 8 << 20)
            return;

        final Wire bufferWire = appender.wire();
        if (bufferWire == null)
            return;

        BytesStore<?, ?> bytes = bufferWire.bytes().bytesStore();
        sync(bytes, start, length);
        this.lastPageSynced += length;
    }

    /**
     * Synchronizes the specified range of memory pages to disk.
     *
     * @param bytes  The bytes store from which to sync.
     * @param start  The start address of the memory to sync.
     * @param length The length of the memory region to sync.
     */
    private void sync(BytesStore<?, ?> bytes, long start, long length) {
        if (!bytes.inside(start, length))
            return;
//        long a = System.nanoTime();
        PosixAPI.posix().msync(bytes.addressForRead(start), length, MSyncFlag.MS_ASYNC);
//        System.out.println("sync took " + (System.nanoTime() - a) / 1000);
    }

    /**
     * Attempts to touch a memory page by performing a compare-and-swap operation on it.
     * This method ensures the page is actively touched and handled by the system.
     *
     * @param nextPage The memory address of the next page to touch.
     * @param bs       The bytes store associated with the memory.
     * @return {@code true} if the page was touched successfully, {@code false} otherwise.
     */
    protected boolean touchPage(long nextPage, BytesStore<?, ?> bs) {
        return bs.compareAndSwapLong(nextPage, 0, 0);
    }
}
