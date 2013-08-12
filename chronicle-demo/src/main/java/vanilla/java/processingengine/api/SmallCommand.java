/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vanilla.java.processingengine.api;

import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptMarshallable;

/**
 * @author peter.lawrey
 */
public class SmallCommand implements ExcerptMarshallable {
    public final StringBuilder clientOrderId = new StringBuilder();
    public String instrument;
    public double price;
    public int quantity;
    public Side side;

    @Override
    public void readMarshallable(Excerpt in) throws IllegalStateException {
        // changes often.
        in.readUTF(clientOrderId);
        // cachable.
        instrument = in.readEnum(String.class);
        price = in.readDouble();
        quantity = in.readInt();
        side = in.readEnum(Side.class);
    }

    @Override
    public void writeMarshallable(Excerpt out) {
        out.writeUTF(clientOrderId);
        out.writeEnum(instrument);
        out.writeDouble(price);
        out.writeInt(quantity);
        out.writeEnum(side);
    }
}
