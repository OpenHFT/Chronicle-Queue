package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.wire.Wire;
import net.openhft.posix.MSyncFlag;
import net.openhft.posix.PosixAPI;

public class MicroToucher {
    private final StoreAppender appender;
    private long lastPageTouched = 0;
    private volatile long lastPageToSync = 0;
    private long lastPageSynced = 0;

    public MicroToucher(StoreAppender appender) {
        this.appender = appender;
    }

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
                final BytesStore bs = bytes.bytesStore();
                if (bs.inside(nextPage, 8))
                    touchPage(nextPage, bs);
            } catch (Throwable ignored) {
            }
            return true;
        }

        lastPageToSync = lastPage;
        return false;
    }

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

        BytesStore bytes = bufferWire.bytes().bytesStore();
        sync(bytes, start, length);
        this.lastPageSynced += length;
    }

    private void sync(BytesStore bytes, long start, long length) {
        if (!bytes.inside(start, length))
            return;
//        long a = System.nanoTime();
        PosixAPI.posix().msync(bytes.addressForRead(start), length, MSyncFlag.MS_ASYNC);
//        System.out.println("sync took " + (System.nanoTime() - a) / 1000);
    }

    protected boolean touchPage(long nextPage, BytesStore bs) {
        return bs.compareAndSwapLong(nextPage, 0, 0);
    }
}
