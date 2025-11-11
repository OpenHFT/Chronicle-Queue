/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.micros;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

class TopOfBookPrice extends SelfDescribingMarshallable {
    private static final long TIMESTAMP_LIMIT = TimeUnit.SECONDS.toMillis(1000);
    final String symbol;
    private long timestamp;
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

    public boolean combine(@NotNull SidedPrice price) {
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
