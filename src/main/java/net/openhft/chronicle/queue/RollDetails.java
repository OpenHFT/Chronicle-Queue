/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.wire.AbstractMarshallable;

public class RollDetails extends AbstractMarshallable {
    final int cycle;
    final long epoch;

    public RollDetails(int cycle, long epoch) {
        this.cycle = cycle;
        this.epoch = epoch;
    }

    public int cycle() {
        return cycle;
    }

    // generated for performance
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        RollDetails that = (RollDetails) o;

        if (cycle != that.cycle) return false;
        return epoch == that.epoch;
    }

    // generated for performance
    @Override
    public int hashCode() {
        int epoch32 = (int) (epoch ^ (epoch >>> 32));
        return cycle * 1019 + epoch32 * 37;
    }
}
