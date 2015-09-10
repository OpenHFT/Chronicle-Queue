/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.impl.AbstractChronicleQueue;
import net.openhft.chronicle.wire.WireUtil;

class SingleChronicleQueue extends AbstractChronicleQueue<SingleChronicleQueueFormat> {
    private final SingleChronicleQueueBuilder builder;

    protected SingleChronicleQueue(final SingleChronicleQueueBuilder builder) {
        super(new SingleChronicleQueueFormat(builder));

        this.builder = builder;

        init();
    }

    // *************************************************************************
    //
    // *************************************************************************

    protected void init() {
        WireUtil.writeDataOnce(null, true, format());
        //Wires.writeDataOnce(wire, true, this::buildHeader);
        //readHeader();
    }
}
