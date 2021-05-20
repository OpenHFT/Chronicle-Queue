package net.openhft.chronicle.queue.benchmark;

import net.openhft.chronicle.bytes.MethodId;
import net.openhft.chronicle.bytes.MethodWriterListener;
import net.openhft.chronicle.bytes.UpdateInterceptor;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.wire.*;

import java.util.function.Supplier;

public final class BookUpdateListenerMethodIdsBinarylightMethodWriter2 implements BookUpdateListener, MethodWriter {

    // result
    private transient final Closeable closeable;
    private transient Supplier<MarshallableOut> out;

    // constructor
    public BookUpdateListenerMethodIdsBinarylightMethodWriter2(Supplier<MarshallableOut> out, Closeable closeable, MethodWriterListener methodWriterListener, UpdateInterceptor updateInterceptor) {
        this.out = out;
        this.closeable = closeable;
    }

    @Override
    public void marshallableOut(MarshallableOut out) {
        this.out = () -> out;
    }

    @MethodId(1)
    public void bookUpdate(final BookUpdate bookUpdate) {
        try (final WriteDocumentContext dc = (WriteDocumentContext) this.out.get().acquireWritingDocument(false)) {
            try {
           dc.chainedElement(false);if (out.get().recordHistory()) MessageHistory.writeHistory(dc);
             final ValueOut valueOut = dc.wire().writeEventId("bookUpdate", 1);
              valueOut.object(BookUpdate.class, bookUpdate);
              //  valueOut.bytesMarshallable(bookUpdate);

            }catch (Throwable t) {
                dc.rollbackOnClose();
                throw Jvm.rethrow(t);
            }
        }
    }

}