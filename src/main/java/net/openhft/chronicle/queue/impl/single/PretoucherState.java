package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;

import java.util.function.LongSupplier;

/**
 * Created by peter on 25/11/2016.
 */
class PretoucherState {
    private static final int HEAD_ROOM = 1 << 20;
    private final LongSupplier posSupplier;
    private long lastTouchedPage = 0, lastTouchedPos = 0;
    private int lastBytesHashcode = 0;

    public PretoucherState(LongSupplier posSupplier) {
        this.posSupplier = posSupplier;
    }

    public void pretouch(MappedBytes bytes) {
        long pos = posSupplier.getAsLong();
        // don't retain the bytes object when it is head so keep the hashCode instead.
        // small risk of a duplicate hashCode.
        if (lastBytesHashcode != System.identityHashCode(bytes)) {
            lastTouchedPage = pos - pos % OS.pageSize();
            lastTouchedPos = pos;
            lastBytesHashcode = System.identityHashCode(bytes);
            String message = bytes.mappedFile().file() + "Reset pretoucher to pos " + pos;
            Jvm.debug().on(getClass(), message);

        } else if (pos >= lastTouchedPage + OS.pageSize()) {
            long headroom = Math.max(HEAD_ROOM, (pos - lastTouchedPos) * 4); // for the next 4 ticks.
            long last = pos + headroom;
            Thread thread = Thread.currentThread();
            int count = 0, pretouch = 0;
            for (; lastTouchedPage < last; lastTouchedPage += OS.pageSize()) {
                if (thread.isInterrupted())
                    break;
                if (bytes.readVolatileLong(lastTouchedPage) == 0)
                    pretouch++;
                count++;
            }
            if (pretouch < count)
                Jvm.debug().on(getClass(), "pretouch for only " + pretouch + " of " + count);

            long pos2 = posSupplier.getAsLong();
            if (Jvm.isDebugEnabled(getClass())) {

                String message = bytes.mappedFile().file() + ": Advanced " + (pos - lastTouchedPos) / 1024 + " KB between pretouch() and " + (pos2 - pos) / 1024 + " KB while mapping of " + headroom / 1024 + " KB.";
                Jvm.debug().on(getClass(), message);
            }
            lastTouchedPos = pos;
        }
    }
}
