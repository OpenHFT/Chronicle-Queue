package net.openhft.chronicle.queue.incubator.streaming.demo.accumulation;

import net.openhft.chronicle.wire.Base85LongConverter;
import net.openhft.chronicle.wire.LongConversion;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public final class MarketData extends SelfDescribingMarshallable {

    @LongConversion(Base85LongConverter.class)
    private long symbol;
    private double last;
    private double high;
    private double low;

    public MarketData() {
    }

    public MarketData(long symbol, double last, double high, double low) {
        symbol(symbol);
        last(last);
        high(high);
        low(low);
    }

    public MarketData(String symbol, double last, double high, double low) {
        symbol(requireNonNull(symbol));
        last(last);
        high(high);
        low(low);
    }

    public String symbol() {
        return Base85LongConverter.INSTANCE.asString(symbol);
    }

    public long symbolAsLong() {
        return symbol;
    }

    public void symbol(long symbol) {
        this.symbol = symbol;
    }

    public void symbol(String symbol) {
        this.symbol = Base85LongConverter.INSTANCE.parse(symbol);
    }

    public double last() {
        return last;
    }

    public void last(double last) {
        this.last = last;
    }

    public double high() {
        return high;
    }

    public void high(double high) {
        this.high = high;
    }

    public double low() {
        return low;
    }

    public void low(double low) {
        this.low = low;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        MarketData that = (MarketData) o;

        if (symbol != that.symbol) return false;
        if (Double.compare(that.last, last) != 0) return false;
        if (Double.compare(that.high, high) != 0) return false;
        return Double.compare(that.low, low) == 0;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        long temp;
        result = 31 * result + (int) (symbol ^ (symbol >>> 32));
        temp = Double.doubleToLongBits(last);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(high);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(low);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}