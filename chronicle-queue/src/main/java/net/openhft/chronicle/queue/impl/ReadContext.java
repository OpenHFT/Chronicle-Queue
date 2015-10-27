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
    public final VanillaBytes bytes;
    public final WireIn wire;

    private long position;

    public ReadContext(@NotNull WireType wireType) {
        this.bytes = VanillaBytes.vanillaBytes();
        this.wire = wireType.apply(this.bytes);
        this.position = 0;
    }

    public WireIn wire(long position, long size) {
        this.bytes.readPosition(position);
        this.bytes.readLimit(position + size);
        this.bytes.writePosition(position + size);

        return this.wire;
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
        if(store != this.bytes.bytesStore()) {
            this.bytes.bytesStore(store, position, size);
        }

        this.position = position;

        return this;
    }
}
