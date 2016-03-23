/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.queue;

/**
 * Created by Peter on 05/03/2016.
 */
public enum TailerDirection {
    NONE(0), // don't move after a read.
    FORWARD(+1), // move to the next entry
    BACKWARD(-1) // move to the previous entry.
    ;

    private final int add;

    TailerDirection(int add) {
        this.add = add;
    }

    public int add() {
        return add;
    }
}
