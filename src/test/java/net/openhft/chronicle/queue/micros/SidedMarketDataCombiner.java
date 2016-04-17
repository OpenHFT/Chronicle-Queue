package net.openhft.chronicle.queue.micros;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by peter on 22/03/16.
 */
public class SidedMarketDataCombiner implements SidedMarketDataListener {
    final MarketDataListener mdListener;
    final Map<String, TopOfBookPrice> priceMap = new TreeMap<>();

    public SidedMarketDataCombiner(MarketDataListener mdListener) {
        this.mdListener = mdListener;
    }

    public void onSidedPrice(SidedPrice sidedPrice) {
        TopOfBookPrice price = priceMap.computeIfAbsent(sidedPrice.symbol, TopOfBookPrice::new);
        if (price.combine(sidedPrice))
            mdListener.onTopOfBookPrice(price);
    }
}
