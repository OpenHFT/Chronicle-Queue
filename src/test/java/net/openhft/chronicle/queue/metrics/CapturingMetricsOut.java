/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.metrics;

import net.openhft.chronicle.wire.metrics.CounterMetric;
import net.openhft.chronicle.wire.metrics.GaugeMetric;
import net.openhft.chronicle.wire.metrics.HistogramMetric;
import net.openhft.chronicle.wire.metrics.Metric;
import net.openhft.chronicle.wire.metrics.MetricsOut;
import net.openhft.chronicle.wire.metrics.PointEvent;
import net.openhft.chronicle.wire.metrics.RateMetric;

import java.util.ArrayList;
import java.util.List;

/**
 * Test helper: captures every metric event as a deep copy (emitters reuse their DTOs) plus the
 * emitting method's name, in arrival order. Also captures {@code metricsFormat} header events
 * so a single instance can observe a whole metrics queue in order.
 */
class CapturingMetricsOut implements MetricsOut, MetricsFormatListener {
    final List<String> methods = new ArrayList<>();
    final List<Object> events = new ArrayList<>();

    @Override
    public void metricsFormat(MetricsFormat metricsFormat) {
        methods.add("metricsFormat");
        events.add(metricsFormat.deepCopy());
    }

    @Override
    public void counterMetric(CounterMetric metric) {
        methods.add("counterMetric");
        events.add(metric.deepCopy());
    }

    @Override
    public void gaugeMetric(GaugeMetric metric) {
        methods.add("gaugeMetric");
        events.add(metric.deepCopy());
    }

    @Override
    public void histogramMetric(HistogramMetric metric) {
        methods.add("histogramMetric");
        events.add(metric.deepCopy());
    }

    @Override
    public void rateMetric(RateMetric metric) {
        methods.add("rateMetric");
        events.add(metric.deepCopy());
    }

    @Override
    public void pointEvent(PointEvent metric) {
        methods.add("pointEvent");
        events.add(metric.deepCopy());
    }

    int size() {
        return methods.size();
    }

    @SuppressWarnings("unchecked")
    <E> E event(int index) {
        return (E) events.get(index);
    }

    /**
     * Returns the last captured event emitted via the given method with the given metric name,
     * or {@code null} if none was captured.
     */
    @SuppressWarnings("unchecked")
    <M extends Metric<M>> M lastMetric(String method, String name) {
        for (int i = methods.size() - 1; i >= 0; i--) {
            if (!methods.get(i).equals(method))
                continue;
            Metric<?> metric = (Metric<?>) events.get(i);
            if (name.equals(metric.name()))
                return (M) metric;
        }
        return null;
    }
}
