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

package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;

public interface CommonStore extends Demarshallable, WriteMarshallable {
    /**
     * @return the file associated with this store.
     */
    @Nullable
    File file();

    @NotNull
    MappedBytes bytes();

    @NotNull
    @Deprecated(/* to be removed in x.26, use dump(WireType) */)
    String dump();

    String dump(WireType wireType);

    @NotNull
    @Deprecated(/* to be removed in x.26, use dump(WireType) */)
    String shortDump();

}
