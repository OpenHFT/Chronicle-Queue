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
 * reading a queue - this protects named tailers that have committed a real (non-zero) index, so a
 * stopped consumer (for example a gateway restarting) does not lose rolls it is known not to have
 * read; retention is then bounded only by free disk and by the keep window for index-0 tailers.
 *
 * <p>Usage: {@code RollFileCleanupMain <rootDir> [options]}
 * <ul>
 *   <li>{@code --keep <N>}         roll-cycle numbers always kept, newest first (default 2 =
 *       current + previous cycle; sparse queues can retain fewer than N files)</li>
 *   <li>{@code --min-free <size>}  free-disk warning floor, e.g. {@code 5G} (default 10G), checked
 *       on every distinct filesystem the root and the queues live on</li>
 *   <li>{@code --delete}           actually delete roll files (default: list candidates only)</li>
 *   <li>{@code --park <name,..>}   one-shot: retire dead/over-lagging named tailers by resetting
 *       their index to 0 so they stop pinning rolls; a restarted consumer then resumes from the
 *       oldest available roll, losing only what retention has since removed. Parking irreversibly
 *       discards the consumer's committed position, so it cannot be combined with {@code --delete}
 *       or {@code --interval}: park first, then sweep with {@code --delete} separately. Replicated
 *       named tailers are refused with a warning; a name found in no queue warns too</li>
 *   <li>{@code --interval <secs>}  loop every N seconds, including fractional seconds, instead of a
 *       single cron-style sweep</li>
 *   <li>{@code --fail-on-warn}     exit non-zero (3) if any retention warning fired</li>
 *   <li>{@code --verbose} / {@code -v}  trace each decision (tailer positions, floor, removals) at
 *       debug level</li>
 * </ul>
 *
 * <p>A queue that fails to open or analyse is warned about and skipped; the remaining queues are
 * still swept. Lag, low-disk, failed-delete, failed-queue, wrong-root, nested-queue-directory and
 * refused-park conditions are logged via {@link net.openhft.chronicle.core.Jvm} ({@code warn}); the
 * {@code --verbose} trace via {@code debug}.
 *
 * <p>Exit codes: {@code 0} success; {@code 1} usage error; {@code 2} the root is not an existing
 * directory; {@code 3} a retention warning fired under {@code --fail-on-warn}.
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
