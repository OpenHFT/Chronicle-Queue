/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.micros;

@FunctionalInterface
public interface OrderListener {
    void onOrder(Order order);
}
