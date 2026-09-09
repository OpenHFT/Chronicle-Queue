/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.InternalAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_HOURLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TailerContinuityTest extends QueueTestCommon {

    private SingleChronicleQueue build(SetTimeProvider tp) {
        return build(getTmpDir(), tp);
    }

    private SingleChronicleQueue build(File dir, SetTimeProvider tp) {
        return SingleChronicleQueueBuilder.binary(dir)
                .timeProvider(tp)
                .rollCycle(TEST_HOURLY)
                .build();
    }

    /**
     * N has data and is NOT sealed (no EOF); N+1 has data. Built with out-of-order (backward)
     * writes so the forward roll never seals N - mirroring the replication back-fill path that
     * leaves a cycle un-EOF'd.
     */
    private void writeUnsealedNWithDataInNPlus1(SingleChronicleQueue q, ExcerptAppender appender) {
        int currentCycle = q.cycle();
        RollCycle rc = q.rollCycle();
        // write N+1 first, so rolling *back* to N suppresses the EOF and leaves N open
        InternalAppender internalAppender = (InternalAppender) appender;
        internalAppender.writeBytes(rc.toIndex(currentCycle + 1, 0), Bytes.from("nPlus1-0"));
        internalAppender.writeBytes(rc.toIndex(currentCycle, 0), Bytes.from("n-0"));
        internalAppender.writeBytes(rc.toIndex(currentCycle, 1), Bytes.from("n-1"));
    }

    /** Read forward until the first non-present document; return how many entries were read. */
    private int drain(ExcerptTailer tailer) {
        int read = 0;
        while (true) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent())
                    return read;
                read++;
            }
        }
    }

    // ---- the failure --------------------------------------------------------------------------

    @Test
    public void tailerShouldReadNewCycle_whenPreviousCycleHasNoEof() {
        SetTimeProvider tp = new SetTimeProvider(TimeUnit.HOURS.toMillis(100));
        try (SingleChronicleQueue q = build(tp);
             ExcerptAppender appender = q.createAppender()) {
            writeUnsealedNWithDataInNPlus1(q, appender);

            try (ExcerptTailer tailer = q.createTailer()) {
                // DESIRED behaviour - FAILS against the current code, which reads only 2 and parks
                // at the un-EOF'd cycle N. A streaming tailer should read N's two entries AND cross
                // into N+1, instead of being stranded by the missing EOF on N.
                assertEquals("tailer should read N's entries and cross into N+1", 3, drain(tailer));
            }
        }
    }

    @Test
    public void tailerShouldReadNewCycle_withPreExistingDataInASingleCycle() {
        File dir = getTmpDir();
        SetTimeProvider tp = new SetTimeProvider(TimeUnit.HOURS.toMillis(100));

        // 1) a queue that ALREADY EXISTS on disk, with data in a single cycle file (no roll, no EOF)
        int preCycle;
        try (SingleChronicleQueue q = build(dir, tp);
             ExcerptAppender appender = q.createAppender()) {
            preCycle = q.cycle();
            appender.writeBytes(Bytes.from("preexisting-0"));
            appender.writeBytes(Bytes.from("preexisting-1"));
        }

        // 2) reopen the existing queue and perform the same backward / back-fill construction,
        //    at a later cycle
        try (SingleChronicleQueue q = build(dir, tp);
             ExcerptTailer tailer = q.createTailer();
             ExcerptAppender appender = q.createAppender()) {
            tp.advanceMillis(TimeUnit.HOURS.toMillis(1));
            assertTrue("run 2 must be a later cycle than the pre-existing one", q.cycle() > preCycle);
            writeUnsealedNWithDataInNPlus1(q, appender);

            // DESIRED behaviour - FAILS against the current code, which reads only 4 and parks
            // at the un-EOF'd cycle N. The tailer should read the 2 pre-existing entries, N's 2
            // entries, AND cross into N+1.
            assertEquals("tailer should read pre-existing + N + N+1", 5, drain(tailer));

            tailer.direction(TailerDirection.BACKWARD).toEnd();
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent());
                assertEquals("nPlus1-0", dc.wire().bytes().toString());
            }
        }
    }

    // ---- recovery: writing/recovering N's EOF un-sticks the tailer ----------------------------

    @Test
    public void tailerRecovers_oncePreviousCyclesAreSealed() {
        SetTimeProvider tp = new SetTimeProvider(TimeUnit.HOURS.toMillis(100));
        try (SingleChronicleQueue q = build(tp);
             ExcerptAppender appender = q.createAppender()) {
            writeUnsealedNWithDataInNPlus1(q, appender);

            try (ExcerptTailer stuck = q.createTailer()) {
                assertEquals(3, drain(stuck));
            }

            // a healthy forward roll (as happens on restart once wall-clock has moved on) seals the
            // earlier cycles - here rolling to N+2 writes the EOFs for both N and N+1
            tp.advanceMillis(TimeUnit.HOURS.toMillis(2));
            appender.writeBytes(Bytes.from("nPlus2-0"));

            try (ExcerptTailer healed = q.createTailer()) {
                // N:0, N:1, N+1:0, N+2:0
                assertEquals("after the EOFs are written the tailer reads every cycle", 4, drain(healed));
            }
        }
    }

    // ---- control: a normally-sealed queue crosses cycles fine ---------------------------------

    @Test
    public void tailerCrosses_whenCyclesAreSealed() {
        SetTimeProvider tp = new SetTimeProvider(TimeUnit.HOURS.toMillis(100));
        int curr;
        File queueDir = getTmpDir();
        try (SingleChronicleQueue q = build(queueDir, tp);
             ExcerptAppender appender = q.createAppender()) {
            // forward writes: the roll into N+1 seals N with an EOF
            appender.writeBytes(Bytes.from("n-0"));
            appender.writeBytes(Bytes.from("n-1"));

            // roll happens while app is closed
            tp.advanceMillis(TimeUnit.HOURS.toMillis(1));

            appender.writeBytes(Bytes.from("n-2"));
            appender.writeBytes(Bytes.from("n-3"));
            curr = q.cycle();

            // 3rd roll
            tp.advanceMillis(TimeUnit.HOURS.toMillis(1));

            try (SingleChronicleQueue q2 = build(queueDir, tp);
                 ExcerptAppender appender2 = q2.createAppender();
                 ExcerptTailer tailer = q2.createTailer()) {
                ((InternalAppender)appender2).writeBytes(q2.rollCycle().toIndex(curr + 1, 0), Bytes.from("n-4"));

                int cycleAfterRoll = q2.cycle();
                assertEquals("roll should have happened while app was closed", curr + 1, cycleAfterRoll);

                assertEquals("a sealed cycle N lets the tailer cross into N+1", 5, drain(tailer));



            }
        }



    }

    // ---- normaliseEOFs() should make the new-cycle data readable ------------------------------

    @Test
    public void tailerShouldRead_afterNormaliseEOFs() {
        SetTimeProvider tp = new SetTimeProvider(TimeUnit.HOURS.toMillis(100));
        try (SingleChronicleQueue q = build(tp);
             ExcerptAppender appender = q.createAppender()) {
            // the backward / back-fill construction leaves the appender positioned ON cycle N
            writeUnsealedNWithDataInNPlus1(q, appender);

            appender.normaliseEOFs();

            try (ExcerptTailer tailer = q.createTailer()) {
                // DESIRED behaviour - FAILS against the current code: normaliseEOFs0 only seals
                // cycles strictly below min(queue.cycle(), appender.cycle()), so with the appender on
                // N it never seals N and the tailer still reads only 2. normaliseEOFs() should make
                // N+1 reachable.
                assertEquals("after normaliseEOFs the tailer should read N+1", 3, drain(tailer));
            }
        }
    }

    // ---- does the directory listing (lastCycle) explain it? -----------------------------------

    @Test
    public void toEnd_alreadyReachesNPlus1_inTheStuckState() {
        // In the stuck state lastCycle() is naturally N+1 - creating N+1 bumped the shared,
        // monotonic table-store highestCycle. So the directory listing is NOT stale, and the
        // random-access toEnd() path already lands in N+1 even while a STREAMING tailer is stuck
        // in N. i.e. for this failure the heal is the missing EOF, not a directory-listing refresh.
        SetTimeProvider tp = new SetTimeProvider(TimeUnit.HOURS.toMillis(100));
        try (SingleChronicleQueue q = build(tp);
             ExcerptAppender appender = q.createAppender()) {
            int n = q.cycle();
            writeUnsealedNWithDataInNPlus1(q, appender);

            assertEquals("lastCycle already knows about N+1", n + 1, q.lastCycle());

            try (ExcerptTailer streaming = q.createTailer()) {
                assertEquals("streaming tailer is stuck in N", 3, drain(streaming));
            }
            try (ExcerptTailer end = q.createTailer()) {
                end.toEnd();
                assertEquals("toEnd() lands in N+1, not N", n + 1, q.rollCycle().toCycle(end.index()));
            }
        }
    }

    // ---- how restart actually heals it --------------------------------------------------------

    @Test
    public void tailerShouldRead_afterRestart() {
        File dir = getTmpDir();
        SetTimeProvider tp = new SetTimeProvider(TimeUnit.HOURS.toMillis(100));

        // run 1: create the stuck state, then "shut down"
        try (SingleChronicleQueue q = build(dir, tp);
             ExcerptAppender appender = q.createAppender()) {
            writeUnsealedNWithDataInNPlus1(q, appender);
        }

        // run 2: reopen (no forward roll). DESIRED behaviour - FAILS against the current code:
        // re-opening alone does not seal N (the constructor positions on N but writes no EOF, and
        // there is no forward-rolling write), so the tailer still reads only 2. A restarted reader
        // should see every entry already on disk (N and N+1).
        try (SingleChronicleQueue q = build(dir, tp);
             ExcerptTailer t = q.createTailer()) {
            assertEquals("after restart the tailer should read every entry on disk", 3, drain(t));
        }
    }

    @Test
    public void restart_thenForwardRoll_sealsN_andTailerRecovers() {
        File dir = getTmpDir();
        SetTimeProvider tp = new SetTimeProvider(TimeUnit.HOURS.toMillis(100));

        try (SingleChronicleQueue q = build(dir, tp);
             ExcerptAppender appender = q.createAppender()) {
            writeUnsealedNWithDataInNPlus1(q, appender);
            try (ExcerptTailer t = q.createTailer()) {
                assertEquals(3, drain(t));
            }
        }

        // run 2: wall-clock has moved on while we were down; the first live write rolls forward and
        // rollCycleTo writes the EOF on N (and N+1) -> the tailer can now cross
        tp.advanceMillis(TimeUnit.HOURS.toMillis(5));
        try (SingleChronicleQueue q = build(dir, tp);
             ExcerptAppender appender = q.createAppender()) {
            appender.writeBytes(Bytes.from("live-after-restart"));   // forward roll seals N (and N+1)
            try (ExcerptTailer t = q.createTailer()) {
                assertTrue("after the post-restart forward roll the tailer crosses past N",
                        drain(t) >= 3);
            }
        }
    }
}
