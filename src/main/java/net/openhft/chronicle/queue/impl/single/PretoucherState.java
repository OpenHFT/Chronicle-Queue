package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;

import java.io.File;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Created by peter on 25/11/2016.
 */
class PretoucherState {
    private static final int HEAD_ROOM = 1 << 20;
    private final LongSupplier posSupplier;
    private final int minHeadRoom;
    private long lastTouchedPage = 0, lastTouchedPos = 0;
    private int lastBytesHashcode = -1;
    private long averageMove = 0;

    public PretoucherState(LongSupplier posSupplier) {
        this(posSupplier, HEAD_ROOM);
    }

    public PretoucherState(LongSupplier posSupplier, int minHeadRoom) {
        this.posSupplier = posSupplier;
        this.minHeadRoom = minHeadRoom;
    }

    static File getFile(MappedBytes bytes) {
        return Optional.ofNullable(bytes)
                .map(MappedBytes::mappedFile)
                .map(MappedFile::file)
                .orElse(new File("none"));
    }

    public void pretouch(MappedBytes bytes) {
        long pos = posSupplier.getAsLong();
        // don't retain the bytes object when it is head so keep the hashCode instead.
        // small risk of a duplicate hashCode.
        if (lastBytesHashcode != System.identityHashCode(bytes)) {
            lastTouchedPage = pos - pos % OS.pageSize();
            lastTouchedPos = pos;
            lastBytesHashcode = System.identityHashCode(bytes);
            String message = getFile(bytes) + " - Reset pretoucher to pos " + pos;
            debug(message);

        } else {
            averageMove = (pos - lastTouchedPage) / 4 + averageMove * 3 / 4;
            long neededHeadRoom = Math.max(minHeadRoom, averageMove * 4); // for the next 4 ticks.
            final long neededEnd = pos + neededHeadRoom;
            if (lastTouchedPage < neededEnd) {
                Thread thread = Thread.currentThread();
                int count = 0, pretouch = 0;
                for (; lastTouchedPage < neededEnd; lastTouchedPage += OS.pageSize()) {
                    if (thread.isInterrupted())
                        break;
                    if (touchPage(bytes, lastTouchedPage))
                        pretouch++;
                    count++;
                }
                if (pretouch < count)
                    debug("pretouch for only " + pretouch + " of " + count);

                long pos2 = posSupplier.getAsLong();
                if (Jvm.isDebugEnabled(getClass())) {
                    String message = getFile(bytes) + ": Advanced " + (pos - lastTouchedPos) / 1024 + " KB between pretouch() and " + (pos2 - pos) / 1024 + " KB while mapping of " + neededHeadRoom / 1024 + " KB.";
                    debug(message);
                }
                lastTouchedPos = pos;
            }
        }
    }

    protected void debug(String message) {
        Jvm.debug().on(getClass(), message);
    }

    protected boolean touchPage(MappedBytes bytes, long offset) {
        return bytes.readVolatileLong(offset) == 0;
    }
}
