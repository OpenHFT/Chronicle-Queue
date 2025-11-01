/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.internal.main.InternalUnlockMain;

/**
 * UnlockMain is an entry point for unlocking resources or files used by a Chronicle Queue.
 * <p>This utility handles the unlocking of locked resources, such as files, that may be in use by the queue.
 */
public final class UnlockMain {

    /**
     * The main method that triggers the unlocking process.
     * Delegates execution to {@link InternalUnlockMain#main(String[])}.
     *
     * @param args Command-line arguments for unlocking operations
     */
    public static void main(String[] args) {
        InternalUnlockMain.main(args);
    }
}
