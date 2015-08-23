/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 */
package net.openhft.chronicle.tcp;

import net.openhft.chronicle.tcp.network.SessionDetailsProvider;
import net.openhft.chronicle.tcp.network.TcpHandler;
import net.openhft.lang.io.Bytes;

import java.util.ArrayList;
import java.util.List;

public class TcpPipeline implements TcpHandler {

    private final List<TcpHandler> handlers = new ArrayList<>();

    private final PipelineCoordinator coordinator = new PipelineCoordinator();

    private boolean busy;

    public void addHandler(TcpHandler handler) {
        handlers.add(handler);
    }

    public static TcpPipeline pipeline(TcpHandler... handlers) {
        final TcpPipeline pipeline = new TcpPipeline();
        if (handlers != null) {
            for (int i = 0; i < handlers.length; i++) {
                pipeline.addHandler(handlers[i]);
            }
        }
        return pipeline;
    }

    @Override
    public boolean process(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
        if (handlers.isEmpty()) {
            return false;
        }

        busy = false;
        sessionDetailsProvider.set(PipelineContext.class, coordinator);
        coordinator.reset();
        coordinator.next(in, out, sessionDetailsProvider);
        return busy;
    }

    @Override
    public void onEndOfConnection(SessionDetailsProvider sessionDetailsProvider) {
        final List<TcpHandler> handlers = this.handlers;
        final int size = handlers.size();
        if (size == 0) {
            return;
        }

        for (int i = 0; i < size; i++) {
            handlers.get(i).onEndOfConnection(sessionDetailsProvider);
        }
    }

    private class PipelineCoordinator implements PipelineContext {

        private int idx;

        @Override
        public void next(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
            final List<TcpHandler> handlers = TcpPipeline.this.handlers;
            if (idx < handlers.size()) {
                final TcpHandler nextHandler = handlers.get(idx++);
                busy |= nextHandler.process(in, out, sessionDetailsProvider);
                idx--;
            }
        }

        public void reset() {
            idx = 0;
        }
    }
}
