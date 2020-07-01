package net.openhft.chronicle.queue.micros;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.bytes.MethodWriterListener;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.wire.*;

public final class MarketDataListenerBinary_lightMethodWriter implements net.openhft.chronicle.queue.micros.MarketDataListener, net.openhft.chronicle.wire.SharedDocumentContext {

    // result
    private transient final Closeable closeable;
    private transient final MethodWriterListener methodWriterListener;
    private transient final MarshallableOut out;
    private transient ThreadLocal<DocumentContextHolder> documentContextTL = ThreadLocal.withInitial(DocumentContextHolder::new);

    // constructor
    public MarketDataListenerBinary_lightMethodWriter(MarshallableOut out, Closeable closeable, MethodWriterListener methodWriterListener) {
        this.methodWriterListener = methodWriterListener;
        this.out = out;
        this.closeable = closeable;
    }

    // method documentContext
    @Override
    public <T extends SharedDocumentContext> T documentContext(final ThreadLocal<DocumentContextHolder> documentContextTL) {
        this.documentContextTL = documentContextTL;
        return (T) this;
    }

    public void onTopOfBookPrice(final net.openhft.chronicle.queue.micros.TopOfBookPrice price) {
        final DocumentContext dc = GenerateMethodWriter.acquireDocumentContext(false, this.documentContextTL, this.out);
        // record history
        if (out.recordHistory()) {
            dc.wire().writeEventName(MethodReader.HISTORY).marshallable(MessageHistory.get());
        }
        if (dc.wire().bytes().retainsComments()) {
            dc.wire().bytes().comment("onTopOfBookPrice");
        }
        final ValueOut valueOut = dc.wire().writeEventName("onTopOfBookPrice");
        if (dc.wire().bytes().retainsComments()) {
            GenerateMethodWriter.addComment(dc.wire().bytes(), price);
        }
        if (price.getClass() == net.openhft.chronicle.queue.micros.TopOfBookPrice.class) {
            valueOut.marshallable(price);
        } else {
            valueOut.object(price);
        }
        dc.close();
    }

}