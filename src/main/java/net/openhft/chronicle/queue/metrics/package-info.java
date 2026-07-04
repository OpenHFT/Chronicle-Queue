/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
/**
 * Chronicle Queue's integration with the {@code net.openhft.chronicle.wire.metrics} API:
 * <ul>
 *     <li>{@link net.openhft.chronicle.queue.metrics.QueueMetricsBinding} - the production
 *     binding that writes metric events to a dedicated Chronicle Queue via a
 *     {@code MetricsOut} method writer, headed by a
 *     {@link net.openhft.chronicle.queue.metrics.MetricsFormat} event, with a
 *     drop-never-throw failure policy (drops are re-exposed as the
 *     {@code chronicle_queue_metrics_dropped_total} self-metric under
 *     {@code chronicle.queue.metrics});</li>
 *     <li>{@link net.openhft.chronicle.queue.metrics.QueueMetrics} - the queue's own touch
 *     points ({@code chronicle.queue.appender}: per-queue appends counter, write-latency
 *     histogram and roll counter, each labelled {@code queue=<directory-basename>}), inert
 *     unless a binding is installed and the source enabled <em>at queue/appender
 *     construction time</em> (the resolve-once, startup-only policy - install the binding
 *     first).</li>
 * </ul>
 * See {@code docs/queue-metrics.adoc} for the metric catalogue and retention guidance.
 */
package net.openhft.chronicle.queue.metrics;
