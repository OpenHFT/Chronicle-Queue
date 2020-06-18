/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue.micros;

import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.TreeMap;

public class OrderManager implements MarketDataListener, OrderIdeaListener {
    final OrderListener orderListener;
    final Map<String, TopOfBookPrice> priceMap = new TreeMap<>();
    final Map<String, OrderIdea> ideaMap = new TreeMap<>();

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
