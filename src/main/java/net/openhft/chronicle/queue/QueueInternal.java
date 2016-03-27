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

import net.openhft.chronicle.core.pool.StringInterner;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.Wires;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by peter on 27/03/16.
 */
enum QueueInternal {
    ;
    static final StringInterner INTERNER = new StringInterner(128);

    static Map<String, Object> readMap(ExcerptTailer tailer) {
        try (DocumentContext context = tailer.readingDocument()) {
            if (!context.isData())
                return null;
            LinkedHashMap<String, Object> map = new LinkedHashMap<>();
            StringBuilder sb = Wires.acquireStringBuilder();
            Wire wire = context.wire();
            while (wire.hasMore()) {
                Object object = wire.readEventName(sb).object();
                map.put(INTERNER.intern(sb), object);
            }
            return map;
        }

    }

    static void writeMap(ExcerptAppender appender, Map<String, Object> map) {
        try (DocumentContext context = appender.writingDocument()) {
            Wire wire = context.wire();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                wire.writeEventName(entry.getKey()).object(entry.getValue());
            }
        }
    }
}
