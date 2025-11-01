/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.micros;

import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.TreeMap;

public class SidedMarketDataCombiner implements SidedMarketDataListener {
    final MarketDataListener mdListener;
    final Map<String, TopOfBookPrice> priceMap = new TreeMap<>();

    public SidedMarketDataCombiner(MarketDataListener mdListener) {
        this.mdListener = mdListener;
    }

    @Override
    public void onSidedPrice(@NotNull SidedPrice sidedPrice) {
        TopOfBookPrice price = priceMap.computeIfAbsent(sidedPrice.symbol, TopOfBookPrice::new);
        if (price.combine(sidedPrice))
            mdListener.onTopOfBookPrice(price);
    }
}
