/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.Nullable;

public interface ExcerptContext {
    @Nullable
    Wire wire();

    @Nullable
    Wire wireForIndex();

    long timeoutMS();

}
