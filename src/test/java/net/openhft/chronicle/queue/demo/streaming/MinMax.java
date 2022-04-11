package net.openhft.chronicle.queue.demo.streaming;

public final class MinMax {

    private double min = Double.MAX_VALUE;
    private double max = Double.MIN_VALUE;

    public MinMax() {
    }

    public MinMax(MarketData marketData) {
        this();
        merge(marketData);
    }

    public synchronized double min() {
        return min;
    }

    public synchronized double max() {
        return max;
    }

    synchronized MinMax merge(final MinMax other) {
        min = Math.min(this.min, other.min);
        max = Math.max(this.max, other.max);
        return this;
    }

    synchronized MinMax merge(final MarketData marketData) {
        min = Math.min(this.min, marketData.last());
        max = Math.max(this.max, marketData.last());
        return this;
    }

    @Override
    public String toString() {
        return "MinMax{" +
                "min=" + min +
                ", max=" + max +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MinMax minMax = (MinMax) o;

        if (Double.compare(minMax.min, min) != 0) return false;
        return Double.compare(minMax.max, max) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(min);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(max);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
