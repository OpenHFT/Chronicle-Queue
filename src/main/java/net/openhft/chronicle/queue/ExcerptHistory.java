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


import net.openhft.chronicle.wire.Marshallable;

/**
 * Created by peter on 27/03/16.
 */
public interface ExcerptHistory extends Marshallable {
    /**
     * Get the ExcerptHistory to update it or read it.
     *
     * @return the ExcerptHistory for the current Excerpt.
     */
    static ExcerptHistory get() {
        return VanillaExcerptHistory.getThreadLocal();
    }

    /**
     * You only need to call this if you wish to override it's behaviour.
     *
     * @param md to change to the default implementation for this thread.
     */
    static void set(ExcerptHistory md) {
        VanillaExcerptHistory.setThreadLocal(md);
    }

    int timings();

    long timing(int n);

    int sources();

    int sourceId(int n);

    long sourceIndex(int n);

    void reset();
}
