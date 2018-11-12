/*
 * Copyright 2014-2018 Chronicle Software
 *
 * http://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

public interface Metadata extends Demarshallable, WriteMarshallable {

    default <T extends Metadata> void overrideFrom(T metadata) {
    }

    enum NoMeta implements Metadata {
        INSTANCE;
        NoMeta() {}
        @SuppressWarnings("unused")
        NoMeta(@NotNull WireIn in) {}
        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
        }
    }
}
