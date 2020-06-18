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

import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

public class TopOfBookPrice extends SelfDescribingMarshallable {
    public static final long TIMESTAMP_LIMIT = TimeUnit.SECONDS.toMillis(1000);
    final String symbol;
    long timestamp;
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
