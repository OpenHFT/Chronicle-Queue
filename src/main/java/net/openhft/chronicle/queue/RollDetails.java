/*
 * Copyright 2016-2020 Chronicle Software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.wire.BytesInBinaryMarshallable;
import org.jetbrains.annotations.Nullable;

public class RollDetails extends BytesInBinaryMarshallable {
    final int cycle;
    final long epoch;

    public RollDetails(int cycle, long epoch) {
        this.cycle = cycle;
        this.epoch = epoch;
    }

    public int cycle() {
        return cycle;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RollDetails that = (RollDetails) o;
        return cycle == that.cycle && epoch == that.epoch;
    }

    @Override
    public int hashCode() {
        int epoch32 = (int) (epoch ^ (epoch >>> 32));
        return cycle * 1019 + epoch32 * 37;
    }
}
