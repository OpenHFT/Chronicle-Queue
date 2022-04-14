package net.openhft.chronicle.queue.internal.streaming;

import net.openhft.chronicle.queue.AppenderListener;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import static java.util.Objects.requireNonNull;

public final class AccumulationUtil {

    // Suppresses default constructor, ensuring non-instantiability.
    private AccumulationUtil() {
    }

    public static long accept(@NotNull final AppenderListener appenderListener,
                              @NotNull final ExcerptTailer tailer) {
        requireNonNull(tailer);
        long lastIndex = -1;
        boolean end = false;
        while (!end) {
            try (final DocumentContext dc = tailer.readingDocument()) {
                final Wire wire = dc.wire();
                if (dc.isPresent() && wire != null) {
                    lastIndex = dc.index();
                    appenderListener.onExcerpt(wire, lastIndex);
                } else {
                    end = true;
                }
            }
        }
        return lastIndex;
    }


}