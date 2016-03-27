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
 * Created by peter on 27/03/16.
 */
public enum NoExcerptHistory implements ExcerptHistory {
    INSTANCE;

    @Override
    public int timings() {
        return 0;
    }

    @Override
    public long timing(int n) {
        return -1;
    }

    @Override
    public int sources() {
        return 0;
    }

    @Override
    public int sourceId(int n) {
        return -1;
    }

    @Override
    public long sourceIndex(int n) {
        return -1;
    }

    @Override
    public void reset() {

    }
}
