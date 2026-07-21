/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.internal.main.InternalRollFileCleanupMain;

/**
 * RollFileCleanupMain applies roll-file retention by named-tailer position across a directory of Chronicle
 * queues: for each queue it deletes (or, by default, just lists) the roll files that are both older
 * than the last {@code N} cycles and already read past by every registered named tailer, then warns
 * if free disk is low.
 *
 * <p>Unlike {@link RemovableRollFileCandidatesMain} - which protects only processes currently
 * reading a queue - this protects every named tailer by its persisted index, so a stopped consumer
 * (for example a gateway restarting) never loses unread rolls; retention is then bounded only by
 * free disk.
 *
 * <p>Usage: {@code RollFileCleanupMain <rootDir> [options]}
 * <ul>
 *   <li>{@code --keep <N>}         roll-cycle numbers always kept, newest first (default 2 =
 *       current + previous cycle; sparse queues can retain fewer than N files)</li>
 *   <li>{@code --min-free <size>}  free-disk warning floor, e.g. {@code 5G} (default 10G)</li>
 *   <li>{@code --delete}           actually delete and apply {@code --park} metadata changes
 *       (default: list candidates and would-park actions only)</li>
 *   <li>{@code --park <name,..>}   retire dead/over-lagging named tailers when {@code --delete}
 *       is supplied (reset their index to 0) so they stop pinning rolls; a restarted consumer then
 *       resumes from the oldest available roll, losing only what retention has since removed</li>
 *   <li>{@code --interval <secs>}  loop every N seconds, including fractional seconds, instead of a
 *       single cron-style sweep</li>
 *   <li>{@code --fail-on-warn}     exit non-zero (3) if a lag or disk warning fired</li>
 *   <li>{@code --verbose} / {@code -v}  trace each decision (tailer positions, floor, removals) at
 *       debug level</li>
 * </ul>
 *
 * <p>Lag, low-disk and failed-delete conditions are logged via {@link net.openhft.chronicle.core.Jvm}
 * ({@code warn}); the {@code --verbose} trace via {@code debug}.
 *
 * <p>Cron example (hourly): {@code 0 * * * * java ... RollFileCleanupMain /var/queues --keep 48
 * --min-free 20G --delete --fail-on-warn}.
 */
public final class RollFileCleanupMain {

    private RollFileCleanupMain() {
    }

    /**
     * Delegates to {@link InternalRollFileCleanupMain#main(String[])}.
     *
     * @param args the root directory followed by options
     * @throws InterruptedException if interrupted while sleeping between interval sweeps
     */
    public static void main(String[] args) throws InterruptedException {
        InternalRollFileCleanupMain.main(args);
    }
}
