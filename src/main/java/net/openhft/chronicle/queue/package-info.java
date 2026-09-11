/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
/**
 * Core Chronicle Queue public API.
 * <p>
 * This package exposes the main abstractions for Chronicle Queue,
 * including {@code ChronicleQueue} together with its writer and reader
 * views ({@code ExcerptAppender} and {@code ExcerptTailer}). A queue is
 * an append-only, file-backed log of messages that can be shared
 * between threads, processes and, with Chronicle Queue Enterprise,
 * machines.
 * <p>
 * Writers typically obtain their own appender per thread and follow a
 * single-writer style: appenders and tailers are not thread-safe and
 * must not be shared between concurrent threads. Configuration concerns
 * such as roll cycles, wire formats and indexing strategies are
 * delegated to builder APIs in this package and its sub-packages.
 */
package net.openhft.chronicle.queue;
