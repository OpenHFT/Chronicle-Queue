/*
 * Copyright 2016-2020 chronicle.software
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

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore;
import net.openhft.chronicle.wire.Wire;

import java.util.function.BiFunction;

/**
 * The {@code WireStoreFactory} interface is a functional interface that creates instances of
 * {@link SingleChronicleQueueStore}. It functions as a factory to produce queue stores,
 * utilizing a rolling chronicle queue and a wire to initialize each store.
 *
 * This interface extends {@link BiFunction}, meaning it takes two arguments — a
 * {@link RollingChronicleQueue} and a {@link Wire} — and returns a {@link SingleChronicleQueueStore}.
 */
@FunctionalInterface
public interface WireStoreFactory extends BiFunction<RollingChronicleQueue, Wire, SingleChronicleQueueStore> {
}
