/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.micros;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;

public class Order extends SelfDescribingMarshallable {
    final String symbol;
    final Side side;
    final double limitPrice;
    final double quantity;

    public Order(String symbol, Side side, double limitPrice, double quantity) {
        this.symbol = symbol;
        this.side = side;
        this.limitPrice = limitPrice;
        this.quantity = quantity;
    }
}
