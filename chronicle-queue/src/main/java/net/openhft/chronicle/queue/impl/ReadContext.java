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
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

//TODO: re-engine
public class ReadContext {
    private long position;
    private final WireIn wire;

    public ReadContext(@NotNull WireType wireType) {
        this.wire = wireType.apply(VanillaBytes.vanillaBytes());
        this.position = 0;
    }

    public WireIn wire(long position, long size) {
        VanillaBytes bytes = (VanillaBytes) this.wire.bytes();
        bytes.readPosition(position);
        bytes.readLimit(position + size);
        bytes.writePosition(position + size);

        return this.wire;
    }

    public BytesStore store() {
        return this.wire.bytes().bytesStore();
    }

    public long position() {
        return this.position;
    }

    public ReadContext position(long position) {
        this.position = position;
        return this;
    }

    public ReadContext store(@NotNull BytesStore store, long position) {
        return store(store, position, store.writeLimit() - position);
    }

    public ReadContext store(@NotNull BytesStore store, long position, long size) {
        if(store != store()) {
            ((VanillaBytes) this.wire.bytes()).bytesStore(store, position, size);
        }

        this.position = position;

        return this;
    }
}
