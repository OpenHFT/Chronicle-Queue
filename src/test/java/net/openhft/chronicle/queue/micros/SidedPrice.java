package net.openhft.chronicle.queue.micros;

import net.openhft.chronicle.wire.AbstractMarshallable;

/**
 * Created by peter on 22/03/16.
 */
public class SidedPrice extends AbstractMarshallable {
    String symbol;
    long timestamp;
    Side side;
    double price, quantity;

    public SidedPrice(String symbol, long timestamp, Side side, double price, double quantity) {
        init(symbol, timestamp, side, price, quantity);
    }

    public SidedPrice init(String symbol, long timestamp, Side side, double price, double quantity) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.side = side;
        this.price = price;
        this.quantity = quantity;
        return this;
    }
}
