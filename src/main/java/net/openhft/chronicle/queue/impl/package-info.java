/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
/**
 * Internal Chronicle Queue implementation details.
 * <p>
 * Types in this package implement the storage, roll-cycle and indexing
 * mechanics underlying the public {@code ChronicleQueue} API. They
 * include concrete queue implementations, store management and
 * file-handling utilities.
 * <p>
 * This package is not part of the supported public API surface.
 * Classes may change, move or be removed between releases; applications
 * should prefer the higher-level types in {@code net.openhft.chronicle.queue}
 * unless they have a compelling reason to rely on these internals.
 */
package net.openhft.chronicle.queue.impl;
