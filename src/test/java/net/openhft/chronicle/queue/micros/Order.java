package net.openhft.chronicle.queue.micros;

import net.openhft.chronicle.wire.AbstractMarshallable;

/**
 * Created by peter on 24/03/16.
 */
public class Order extends AbstractMarshallable {
    String symbol;
    Side side;
    double limitPrice, quantity;

    public Order(String symbol, Side side, double limitPrice, double quantity) {
        this.symbol = symbol;
        this.side = side;
        this.limitPrice = limitPrice;
        this.quantity = quantity;
    }
}
