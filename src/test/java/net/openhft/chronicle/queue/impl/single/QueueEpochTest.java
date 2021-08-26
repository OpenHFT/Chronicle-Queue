package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import org.junit.Test;

import java.io.File;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public final class QueueEpochTest extends ChronicleQueueTestBase {
    private static final boolean DEBUG = false;
    private static final long MIDNIGHT_UTC_BASE_TIME = 1504569600000L;
    // 17:15 EDT, 21:15 UTC
    private static final long UTC_OFFSET = TimeUnit.HOURS.toMillis(21) + TimeUnit.MINUTES.toMillis(15);
    private static final long ROLL_TIME = MIDNIGHT_UTC_BASE_TIME + UTC_OFFSET;

    private static final long TEN_MINUTES_BEFORE_ROLL_TIME = ROLL_TIME - TimeUnit.MINUTES.toMillis(10L);
    private static final long FIVE_MINUTES_BEFORE_ROLL_TIME = ROLL_TIME - TimeUnit.MINUTES.toMillis(5L);
    private static final long ONE_SECOND_BEFORE_ROLL_TIME = ROLL_TIME - TimeUnit.SECONDS.toMillis(1L);
    private static final long ONE_SECOND_AFTER_ROLL_TIME = ROLL_TIME + TimeUnit.SECONDS.toMillis(1L);
    private static final long ONE_DAY = TimeUnit.DAYS.toMillis(1L);
    private static final RollCycle DAILY_ROLL = RollCycles.DAILY;

    private long currentTime;

    private static void logDebug(final String format, final Object... args) {
        if (DEBUG) {
           // System.out.printf(format, args);
        }
    }

    @Test
    public void shouldRollQueueFilesAccordingToUtcOffset() {
        logDebug("UTC offset is %dms%n", UTC_OFFSET);
        final File queueDir = getTmpDir();
        final CapturingStoreFileListener fileListener = new CapturingStoreFileListener();
        setCurrentTime(MIDNIGHT_UTC_BASE_TIME);

        try (final RollingChronicleQueue queue = ChronicleQueue.singleBuilder(queueDir).
                rollTime(LocalTime.of(21, 15), ZoneOffset.UTC). // epoch is deprecated in favour of rollTime
                timeProvider(this::getCurrentTime). // override the clock used by the queue to detect roll-over
                storeFileListener(fileListener). // capture file-roll events
                rollCycle(DAILY_ROLL).
                build()) {

            logDebug("Queue epoch offset is %d%n", queue.epoch());
            final ExcerptAppender appender = queue.acquireAppender();
            final TestEvent eventWriter = appender.methodWriter(TestEvent.class);

            setCurrentTime(TEN_MINUTES_BEFORE_ROLL_TIME);
            eventWriter.setOrGetEvent(Long.toString(TEN_MINUTES_BEFORE_ROLL_TIME));

            setCurrentTime(FIVE_MINUTES_BEFORE_ROLL_TIME);
            eventWriter.setOrGetEvent(Long.toString(FIVE_MINUTES_BEFORE_ROLL_TIME));

            setCurrentTime(ONE_SECOND_BEFORE_ROLL_TIME);
            eventWriter.setOrGetEvent(Long.toString(ONE_SECOND_BEFORE_ROLL_TIME));

            assertEquals(0, fileListener.numberOfRollEvents());

            setCurrentTime(ONE_SECOND_AFTER_ROLL_TIME);
            eventWriter.setOrGetEvent(Long.toString(ONE_SECOND_AFTER_ROLL_TIME));

            assertEquals(1, fileListener.numberOfRollEvents());

            setCurrentTime(ONE_SECOND_BEFORE_ROLL_TIME + ONE_DAY);
            eventWriter.setOrGetEvent(Long.toString(ONE_SECOND_BEFORE_ROLL_TIME + ONE_DAY));

            assertEquals(1, fileListener.numberOfRollEvents());

            setCurrentTime(ONE_SECOND_AFTER_ROLL_TIME + ONE_DAY);
            eventWriter.setOrGetEvent(Long.toString(ONE_SECOND_AFTER_ROLL_TIME + ONE_DAY));

            assertEquals(2, fileListener.numberOfRollEvents());
        }
    }

    private long getCurrentTime() {
        return currentTime;
    }

    private void setCurrentTime(final long currentTime) {
        logDebug("Setting current time to %s%n", Instant.ofEpochMilli(currentTime));
        this.currentTime = currentTime;
    }

    @FunctionalInterface
    interface TestEvent {
        void setOrGetEvent(String event);
    }

    private final class CapturingStoreFileListener implements StoreFileListener {
        private int numberOfRollEvents = 0;

        @Override
        public void onAcquired(final int cycle, final File file) {
            logFileAction(cycle, file, "acquired");
        }

        @Override
        public void onReleased(final int cycle, final File file) {
            logFileAction(cycle, file, "released");
            numberOfRollEvents++;
        }

        int numberOfRollEvents() {
            BackgroundResourceReleaser.releasePendingResources();
            return numberOfRollEvents;
        }

        private void logFileAction(final int cycle, final File file, final String action) {
            logDebug("%s file %s for cycle %d at %s%n", action, file.getName(), cycle,
                    Instant.ofEpochMilli(getCurrentTime()));
        }
    }
}