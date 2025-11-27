/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.Nullable;

/**
 * View over the wire and timeout parameters associated with a queue excerpt operation.
 * <p>
 * Implementations expose the current {@link Wire} used for reading or writing, an optional
 * index-aware wire for random access operations, and the timeout in milliseconds that guards
 * blocking calls. Instances are generally short lived and not intended to be shared.
 */
public interface ExcerptContext {
    @Nullable
    Wire wire();

    @Nullable
    Wire wireForIndex();

    @Deprecated(/* to be removed in 2027 */)
    long timeoutMS();
}
