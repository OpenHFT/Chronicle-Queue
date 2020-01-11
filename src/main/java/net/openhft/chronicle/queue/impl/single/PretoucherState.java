package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.function.LongSupplier;

class PretoucherState {
    private static final int HEAD_ROOM = Integer.getInteger("PretoucherState.headRoom", 1 << 20);
    public static final int FACTOR = 32;
    @NotNull
    private final LongSupplier posSupplier;
    private int minHeadRoom;
    private long lastTouchedPage = 0,
            lastTouchedPos = 0,
            lastPos = 0;
    private int lastBytesHashcode = -1;
    private long averageMove = 0;

    public PretoucherState(@NotNull LongSupplier posSupplier) {
        this(posSupplier, HEAD_ROOM);
    }

    public PretoucherState(@NotNull LongSupplier posSupplier, int minHeadRoom) {
        this.posSupplier = posSupplier;
        this.minHeadRoom = minHeadRoom;
    }

    static File getFile(MappedBytes bytes) {
        if (bytes == null)
            return new File("none");

        return bytes.mappedFile().file();
    }

    // cannot make this @NotNull until PretoucherStateTest is fixed to not pass null
    public void pretouch(MappedBytes bytes) {
        final long pos;
        try {
            pos = posSupplier.getAsLong();
        } catch (NullPointerException npe) {
            throw new IllegalStateException("Encountered an NPE, possibly because the store was released by something else", npe);
        }
        // don't retain the bytes object when it is head so keep the hashCode instead.
        // small risk of a duplicate hashCode.
        int pageSize = OS.pageSize();
        if (lastBytesHashcode != System.identityHashCode(bytes)) {
            lastTouchedPage = pos - pos % pageSize;
            lastTouchedPos = pos;
            lastBytesHashcode = System.identityHashCode(bytes);
            averageMove = OS.pageSize();
            lastPos = pos;
            if (Jvm.isDebugEnabled(getClass())) {
                String message = getFile(bytes) + " - Reset pretoucher to pos " + pos + " as the underlying MappedBytes changed.";
                debug(message);
            }
        } else {
            long moved = pos - lastPos;
            averageMove = moved / FACTOR + averageMove * (FACTOR - 1) / FACTOR;
            long neededHeadRoom = Math.max(minHeadRoom, averageMove * FACTOR); // for the next 4 ticks.
            final long neededEnd = pos + neededHeadRoom;
            if (lastTouchedPage < neededEnd) {
                Thread thread = Thread.currentThread();
                int count = 0, pretouch = 0;
                for (; lastTouchedPage < neededEnd; lastTouchedPage += pageSize) {
                    if (thread.isInterrupted())
                        break;
                    if (touchPage(bytes, lastTouchedPage))
                        pretouch++;
                    count++;
                }
                onTouched(count);
                if (pretouch < count) {
                    minHeadRoom += 256 << 10;
                    if (Jvm.isDebugEnabled(getClass()))
                        debug("pretouch for only " + pretouch + " of " + count + " min: " + (minHeadRoom >> 20) + " MB.");
                }

                long pos2 = posSupplier.getAsLong();
                if (Jvm.isDebugEnabled(getClass())) {
                    String message = getFile(bytes) + ": Advanced " + (pos - lastTouchedPos) / 1024 + " KB, " +
                            "avg " + averageMove / 1024 + " KB " +
                            "between pretouch() and " + (pos2 - pos) / 1024 + " KB " +
                            "while mapping of " + pretouch * pageSize / 1024 + " KB ";
                    debug(message);
                }
                lastTouchedPos = pos;
            }
            lastPos = pos;
        }
    }

    protected void debug(String message) {
        Jvm.debug().on(getClass(), message);
    }

    protected boolean touchPage(MappedBytes bytes, long offset) {
        return bytes != null && bytes.compareAndSwapLong(offset, 0L, 0L);
    }

    protected void onTouched(int count) {
    }
}
