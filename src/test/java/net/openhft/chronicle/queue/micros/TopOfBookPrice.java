package net.openhft.chronicle.queue.micros;

import net.openhft.chronicle.wire.AbstractMarshallable;

import java.util.concurrent.TimeUnit;

/**
 * Created by peter on 22/03/16.
 */
public class TopOfBookPrice extends AbstractMarshallable {
    public static final long TIMESTAMP_LIMIT = TimeUnit.SECONDS.toMillis(1000);
    final String symbol;
    long timestamp;
    double buyPrice, buyQuantity;
    double sellPrice, sellQuantity;

    public TopOfBookPrice(String symbol, long timestamp, double buyPrice, double buyQuantity, double sellPrice, double sellQuantity) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.buyPrice = buyPrice;
        this.buyQuantity = buyQuantity;
        this.sellPrice = sellPrice;
        this.sellQuantity = sellQuantity;
    }

    public TopOfBookPrice(String symbol) {
        this.symbol = symbol;
        timestamp = 0;
        buyPrice = sellPrice = Double.NaN;
        buyQuantity = sellQuantity = 0;
    }

    public boolean combine(SidedPrice price) {
        boolean changed = false;
        switch (price.side) {
            case Buy:
                changed = timestamp + TIMESTAMP_LIMIT < price.timestamp ||
                        buyPrice != price.price ||
                        buyQuantity != price.quantity;
                if (changed) {
                    timestamp = price.timestamp;
                    buyPrice = price.price;
                    buyQuantity = price.quantity;
                }
                break;
            case Sell:
                changed = timestamp + TIMESTAMP_LIMIT < price.timestamp ||
                        sellPrice != price.price ||
                        sellQuantity != price.quantity;
                if (changed) {
                    timestamp = price.timestamp;
                    sellPrice = price.price;
                    sellQuantity = price.quantity;
                }
                break;
        }
        return changed;
    }
}
