/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.metrics;

/**
 * The wire interface for the {@code metricsFormat} header event - the first excerpt of a
 * dedicated metrics queue. Consumers add an implementation of this interface alongside their
 * {@link net.openhft.chronicle.wire.metrics.MetricsOut} when building a
 * {@code MethodReader} over a metrics queue, so the header is dispatched like any other event.
 */
@FunctionalInterface
public interface MetricsFormatListener {

    /**
     * Receives the metrics queue's format header.
     *
     * @param metricsFormat the header; the instance may be reused by the caller after return
     */
    void metricsFormat(MetricsFormat metricsFormat);
}
