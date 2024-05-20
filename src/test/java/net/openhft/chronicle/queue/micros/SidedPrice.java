/*
 * Copyright 2016-2020 chronicle.software
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
