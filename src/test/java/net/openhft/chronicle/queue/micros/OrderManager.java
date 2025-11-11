/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.micros;

import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.TreeMap;

public class OrderManager implements MarketDataListener, OrderIdeaListener {
    private final OrderListener orderListener;
    private final Map<String, TopOfBookPrice> priceMap = new TreeMap<>();
    private final Map<String, OrderIdea> ideaMap = new TreeMap<>();

    public OrderManager(OrderListener orderListener) {
        this.orderListener = orderListener;
    }

    @Override
    public void onTopOfBookPrice(@NotNull TopOfBookPrice price) {
        OrderIdea idea = ideaMap.get(price.symbol);
        if (idea != null && placeOrder(price, idea)) {
            // prevent the idea being used again until the strategy asks for more
            ideaMap.remove(price.symbol);
            return;
        }

        price.mergeToMap(priceMap, p -> p.symbol);
    }

    @Override
    public void onOrderIdea(@NotNull OrderIdea idea) {
        TopOfBookPrice price = priceMap.get(idea.symbol);
        if (price != null && placeOrder(price, idea)) {
            // remove the price information until we see a market data update to prevent trading until then.
            priceMap.remove(idea.symbol);
            return;
        }

        idea.mergeToMap(ideaMap, i -> i.symbol);
    }

    private boolean placeOrder(@NotNull TopOfBookPrice price, @NotNull OrderIdea idea) {
        double orderPrice, orderQuantity;
        switch (idea.side) {
            case Buy:
                if (!(price.buyPrice >= idea.limitPrice))
                    return false;
                orderPrice = price.buyPrice;
                orderQuantity = Math.min(price.buyQuantity, idea.quantity);
                break;
            case Sell:
                if (!(price.sellPrice <= idea.limitPrice))
                    return false;
                orderPrice = price.sellPrice;
                orderQuantity = Math.min(price.sellQuantity, idea.quantity);
                break;
            default:
                return false;
        }

        orderListener.onOrder(new Order(idea.symbol, idea.side, orderPrice, orderQuantity));
        return true;
    }
}
