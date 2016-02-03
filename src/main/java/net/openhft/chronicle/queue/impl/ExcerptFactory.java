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
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.Excerpt;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.jetbrains.annotations.NotNull;

public interface ExcerptFactory {
    /**
     * @param queue immutable fields of the queue
     * @return a createExcerpt
     */
    Excerpt createExcerpt(@NotNull ChronicleQueue queue);

    /**
     * @param queue immutable fields of the queue
     * @return a ExcerptTailer
     */
    ExcerptTailer createTailer(@NotNull ChronicleQueue queue);

    /**
     * @param queue immutable fields of the queue
     * @return an ExcerptAppender
     */
    ExcerptAppender createAppender(@NotNull ChronicleQueue queue);
}
