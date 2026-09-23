/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.After;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runners.model.TestTimedOutException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/** Runs the real inherited rolling rule around JUnit's timeout and teardown. */
public class RollingFixtureTimeoutTest {
    private static CountDownLatch restoring;
    private static CountDownLatch releaseRestoration;
    private static CountDownLatch followingStarted;
    private static volatile boolean restored;

    @Test
    public void nextTestWaitsForTimedOutFixtureRestoration() throws Exception {
        restoring = new CountDownLatch(1);
        releaseRestoration = new CountDownLatch(1);
        followingStarted = new CountDownLatch(1);
        restored = false;
        AtomicReference<Result> timedOut = new AtomicReference<>();
        AtomicReference<Result> following = new AtomicReference<>();
        Thread runner = new Thread(() -> {
            JUnitCore junit = new JUnitCore();
            timedOut.set(junit.run(Request.method(TimedFixture.class, "bodyTimesOut")));
            following.set(junit.run(Request.method(FollowingFixture.class, "requiresRestoredState")));
        }, "rolling-timeout-runner");
        runner.start();
        try {
            assertTrue("Timed-out body must reach teardown", restoring.await(5, TimeUnit.SECONDS));
            assertFalse("Next test started while the timed-out fixture still owned shared state",
                    followingStarted.await(200, TimeUnit.MILLISECONDS));
        } finally {
            releaseRestoration.countDown();
            runner.join(5000);
        }
        assertFalse("Nested runner must terminate", runner.isAlive());
        assertTrue(restored);
        assertNotNull(timedOut.get());
        assertEquals(1, timedOut.get().getRunCount());
        assertEquals(timedOut.get().getFailures().toString(), 1, timedOut.get().getFailureCount());
        assertTrue(timedOut.get().getFailures().get(0).getException() instanceof TestTimedOutException);
        assertNotNull(following.get());
        assertEquals(1, following.get().getRunCount());
        assertTrue(following.get().getFailures().toString(), following.get().wasSuccessful());
    }

    public static class TimedFixture extends ChronicleRollingIssueTest {
        public TimedFixture() {
            // Only this synthetic control uses a short timeout. The real fixture's budget is unchanged.
            globalTimeout = Timeout.millis(100);
        }

        @Test
        public void bodyTimesOut() throws InterruptedException {
            new CountDownLatch(1).await();
        }

        @Override
        @After
        public void afterChecks() {
            restoring.countDown();
            // Simulate teardown which cannot return until its resource owner has finished.
            boolean released = false;
            while (!released) {
                try {
                    releaseRestoration.await();
                    released = true;
                } catch (InterruptedException expected) {
                    // The timeout and outer rule both request interruption; neither completes teardown.
                }
            }
            super.afterChecks();
            restored = true;
        }
    }

    public static class FollowingFixture {
        @Test
        public void requiresRestoredState() {
            followingStarted.countDown();
            assertTrue("Previous fixture must restore shared state first", restored);
        }
    }
}
