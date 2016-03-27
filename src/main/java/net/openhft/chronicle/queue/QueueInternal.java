/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
