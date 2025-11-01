/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
