package net.openhft.chronicle.queue.benchmark;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;

public class Rung extends SelfDescribingMarshallable {
    double rate;
    double qty;

    public Rung(double rate, double qty) {
        this.rate = rate;
        this.qty = qty;
    }

    public Rung() {
    }

    public double rate() {
        return rate;
    }

    public Rung rate(double rate) {
        this.rate = rate;
        return this;
    }

    public double qty() {
        return qty;
    }

    public Rung qty(double qty) {
        this.qty = qty;
        return this;
    }
}
