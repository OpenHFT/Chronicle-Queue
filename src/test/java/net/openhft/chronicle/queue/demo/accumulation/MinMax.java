package net.openhft.chronicle.queue.demo.accumulation;

public final class MinMax {
    private double min;
    private double max;

    public synchronized double min() {
        return min;
    }

    public synchronized double max() {
        return max;
    }

    synchronized void accept(final MarketData marketData) {
        min = Math.min(min, marketData.last());
        max = Math.min(max, marketData.last());
    }

    @Override
    public String toString() {
        return "MinMax{" +
                "min=" + min +
                ", max=" + max +
                '}';
    }
}
