package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;

import java.util.function.LongSupplier;

/**
 * Created by peter on 25/11/2016.
 */
public class Pretoucher {
    static final int HEAD_ROOM = 1 << 20;
    private final LongSupplier posSupplier;
    private long lastTouchedPage = 0, lastTouchedPos = 0;

    public Pretoucher(LongSupplier posSupplier) {
        this.posSupplier = posSupplier;
    }

    public void pretouch(MappedBytes bytes) {
        long pos = posSupplier.getAsLong();
        if (lastTouchedPage > pos) {
            lastTouchedPage = pos - pos % OS.pageSize();
            lastTouchedPos = pos;
            String message = "Reset lastTouched to " + lastTouchedPage;
            Jvm.debug().on(getClass(), message);

        } else {
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
                Jvm.debug().on(getClass(), "pretouch for only " + pretouch + " or " + count);

            long pos2 = posSupplier.getAsLong();
            if (Jvm.isDebugEnabled(getClass())) {

                String message = bytes.mappedFile().file() + ": Advanced " + (pos - lastTouchedPos) / 1024 + " KB between pretouch() and " + (pos2 - pos) / 1024 + " KB while mapping of " + headroom / 1024 + " KB.";
                Jvm.debug().on(getClass(), message);
            }
            lastTouchedPos = pos;
        }
    }
}
