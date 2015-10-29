/*
 *
 *    Copyright (C) 2015  higherfrequencytrading.com
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU Lesser General Public License as published by
 *    the Free Software Foundation, either version 3 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Lesser General Public License for more details.
 *
 *    You should have received a copy of the GNU Lesser General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.VanillaBytes;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.bytes.NoBytesStore.noBytesStore;

//TODO: re-engine
public class WriteContext {
    public final VanillaBytes bytes;
    public final WireOut wire;

    public WriteContext(@NotNull WireType wireType) {
        this.bytes = VanillaBytes.vanillaBytes();
        this.wire = wireType.apply(this.bytes);
    }

    public void clear() {
        this.bytes.bytesStore(noBytesStore(), 0, 0);
    }
}
