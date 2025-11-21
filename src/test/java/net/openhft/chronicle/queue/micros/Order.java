/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.micros;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;

public class Order extends SelfDescribingMarshallable {

    public Order(String symbol, Side side, double limitPrice, double quantity) {
    }
}
