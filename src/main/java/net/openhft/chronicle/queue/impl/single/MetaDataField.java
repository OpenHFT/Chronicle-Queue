/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.WireKey;
import org.jetbrains.annotations.Nullable;

public enum MetaDataField implements WireKey {
    wireType,
    writePosition,
    roll,
    indexing,
    lastAcknowledgedIndexReplicated,
    recovery,
    encodedSequence,
    lastIndexReplicated,
    sourceId,
    dataFormat,
    metadata;

    @Nullable
    @Override
    public Object defaultValue() {
        throw new IORuntimeException("field " + name() + " required");
    }
}
