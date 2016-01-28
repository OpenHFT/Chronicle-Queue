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
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.Excerpt;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.ExcerptFactory;
import net.openhft.chronicle.queue.impl.Excerpts;
import org.jetbrains.annotations.NotNull;

class SingleChronicleQueueExcerptFactory implements ExcerptFactory<SingleChronicleQueue> {
    public static final SingleChronicleQueueExcerptFactory INSTANCE = new SingleChronicleQueueExcerptFactory();

    @Override
    public Excerpt createExcerpt(@NotNull SingleChronicleQueue queue) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ExcerptTailer createTailer(@NotNull SingleChronicleQueue queue) {
        return new Excerpts.StoreTailer(queue);
    }

    @Override
    public ExcerptAppender createAppender(@NotNull SingleChronicleQueue queue) {
        ExcerptAppender appender = new Excerpts.StoreAppender(queue);

        if (queue.buffered()) {
            throw new IllegalStateException(
                    "This is a a commercial feature, please contact sales@higherfrequencytrading" +
                            ".com to unlock this feature, or instead use the " +
                            "'software.chronicle.enterprise.queue.EnterpriseChronicleQueueBuilder'" +
                            "'");
        }

        return appender;
    }
}
