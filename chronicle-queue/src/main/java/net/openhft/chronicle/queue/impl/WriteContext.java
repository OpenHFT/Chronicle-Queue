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

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.VanillaBytes;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

//TODO: re-engine
public class WriteContext {
    private final WireOut wire;

    public WriteContext(@NotNull WireType wireType) {
        this.wire = wireType.apply(VanillaBytes.vanillaBytes());
    }

    public WireOut wire() {
        return this.wire;
    }

    public BytesStore store() {
        return this.wire.bytes().bytesStore();
    }

    public long position() {
        return this.wire.bytes().writePosition();
    }

    public WriteContext position(long position) {
        this.wire.bytes().writePosition(position);
        return this;
    }

    public WriteContext store(@NotNull BytesStore store, long position) {
        return store(store, position, store.writeLimit() - position);
    }

    public WriteContext store(@NotNull BytesStore store, long position, long size) {
        if(store != store()) {
            VanillaBytes bytes = (VanillaBytes) this.wire.bytes();
            bytes.bytesStore(store, position, size);
            bytes.writePosition(position);
            bytes.writeLimit(position + size);
        } else {
            position(position);
        }

        return this;
    }
}
