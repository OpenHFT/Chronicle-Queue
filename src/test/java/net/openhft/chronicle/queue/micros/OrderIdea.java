/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.micros;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;

class OrderIdea extends SelfDescribingMarshallable {
    final String symbol;
    final Side side;
    final double limitPrice;
    final double quantity;

    public OrderIdea(String symbol, Side side, double limitPrice, double quantity) {
        this.symbol = symbol;
        this.side = side;
        this.limitPrice = limitPrice;
        this.quantity = quantity;
    }
}
