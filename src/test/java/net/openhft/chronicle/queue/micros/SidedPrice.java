/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.micros;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.jetbrains.annotations.NotNull;

public class SidedPrice extends SelfDescribingMarshallable {
    String symbol;
    long timestamp;
    Side side;
    double price, quantity;

    @SuppressWarnings("this-escape")
    public SidedPrice(String symbol, long timestamp, Side side, double price, double quantity) {
        init(symbol, timestamp, side, price, quantity);
    }

    @NotNull
    public SidedPrice init(String symbol, long timestamp, Side side, double price, double quantity) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.side = side;
        this.price = price;
        this.quantity = quantity;
        return this;
    }
}
