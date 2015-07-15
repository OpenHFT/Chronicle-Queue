package net.openhft.chronicle.network;

import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.lang.io.Bytes;

import javax.swing.table.TableColumn;
import java.util.ArrayList;
import java.util.List;

public class TcpPipeline implements TcpHandler {

    private final List<TcpHandler> handlers = new ArrayList<>();

    private final PipelineCoordinator coordinator = new PipelineCoordinator();

    public void addHandler(TcpHandler handler) {
        handlers.add(handler);
    }

    public static TcpPipeline pipeline(TcpHandler ... handlers) {
        final TcpPipeline pipeline = new TcpPipeline();
        if (handlers != null) {
            for (int i = 0; i < handlers.length; i++) {
                pipeline.addHandler(handlers[i]);
            }
        }
        return pipeline;
    }

    @Override
    public void process(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
        if (handlers.isEmpty()) {
            return;
        }

        sessionDetailsProvider.set(PipelineContext.class, coordinator);
        coordinator.reset();
        coordinator.next(in, out, sessionDetailsProvider);
    }

    @Override
    public void onEndOfConnection() {

    }

    private class PipelineCoordinator implements PipelineContext {

        private int idx;

        private TcpHandler nextHandler;

        @Override
        public void next(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
            if (idx < handlers.size()) {
                nextHandler = handlers.get(idx++);
                nextHandler.process(in, out, sessionDetailsProvider);
            }
        }

        @Override
        public void cancel() {

        }

        @Override
        public void done() {
            // necessary ?
        }

        public void reset() {
            idx = 0;
        }
    }
}
