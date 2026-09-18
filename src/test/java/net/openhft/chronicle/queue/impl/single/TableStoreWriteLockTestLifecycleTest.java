/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IOTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runners.model.TestTimedOutException;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TableStoreWriteLockTestLifecycleTest {
    private static volatile State state;

    @Test(timeout = 30_000)
    public void closesChildAndMappedFileAfterAssertionFailure() throws Exception {
        checkLifecycle(FailureTestCase.class, AssertionError.class);
    }

    @Test(timeout = 30_000)
    public void closesChildAndMappedFileWhileTimedOutBodyIsStillBlocked() throws Exception {
        checkLifecycle(TimeoutTestCase.class, TestTimedOutException.class);
    }

    @Test(timeout = 30_000)
    public void closesChildAndMappedFileAfterSuccessfulBody() throws Exception {
        checkLifecycle(SuccessTestCase.class, null);
    }

    @Test(timeout = 30_000)
    public void attachesCleanupEvidenceWithoutReplacingBodyFailure() throws Exception {
        checkLifecycle(DiagnosticFailureTestCase.class, AssertionError.class);
    }

    private static void checkLifecycle(Class<?> testCase, Class<? extends Throwable> failureType) throws Exception {
        State run = new State();
        state = run;
        Throwable originalFailure = null;
        try {
            Result result = JUnitCore.runClasses(testCase);
            if (result.getFailureCount() != (failureType == null ? 0 : 1))
                result.getFailures().forEach(failure -> failure.getException().printStackTrace());
            if (failureType == null) {
                assertTrue(result.getFailures().toString(), result.wasSuccessful());
            } else {
                assertEquals(result.getFailures().toString(), 1, result.getFailureCount());
                assertTrue(result.getFailures().toString(), failureType.isInstance(result.getFailures().get(0).getException()));
                if (failureType == AssertionError.class)
                    assertSame(run.originalFailure, result.getFailures().get(0).getException());
                if (testCase == DiagnosticFailureTestCase.class) {
                    Throwable[] suppressed = run.originalFailure.getSuppressed();
                    assertEquals(1, suppressed.length);
                    String diagnostic = suppressed[0].getMessage();
                    assertTrue(diagnostic, diagnostic.contains("Lock-holder pid="));
                    assertTrue(diagnostic, diagnostic.contains("aliveAfter=false"));
                    assertTrue(diagnostic, diagnostic.contains(run.directory.toString()));
                }
            }
            assertNotNull("The control must start a real lock-holder JVM", run.child);
            if (testCase == TimeoutTestCase.class)
                assertTrue("The timed-out body must still be blocked during teardown", run.bodyThread.isAlive());
            assertFalse("Fixture teardown left its lock-holder JVM alive", run.child.isAlive());
            assertTrue("Fixture teardown left the parent lock mapping open", run.lock.isClosed());
            assertFalse("Fixture teardown left its mapped store directory", run.directory.toFile().exists());
        } catch (Exception | Error failure) {
            originalFailure = failure;
            throw failure;
        } finally {
            // Independently clean failed negative controls; never leave a JVM holding a CI checkout.
            try {
                cleanNegativeControl(run);
            } catch (Exception | Error cleanupFailure) {
                if (originalFailure == null)
                    throw cleanupFailure;
                originalFailure.addSuppressed(cleanupFailure);
            } finally {
                state = null;
            }
        }
    }

    private static void cleanNegativeControl(State run) throws Exception {
        run.releaseBody.countDown();
        if (run.bodyThread != null && run.bodyThread != Thread.currentThread())
            run.bodyThread.join(5_000);
        if (run.child != null && run.child.isAlive()) {
            run.child.destroyForcibly();
            assertTrue("Negative-control child did not terminate", run.child.waitFor(5, TimeUnit.SECONDS));
        }
        if (run.lock != null)
            run.lock.close();
        if (run.fixture != null)
            run.fixture.tearDown();
        BackgroundResourceReleaser.releasePendingResources();
        if (run.directory != null && run.directory.toFile().exists())
            assertTrue("Negative-control directory did not delete", IOTools.deleteDirWithFiles(run.directory.toFile()));
    }

    public abstract static class FixtureTestCase {
        public FixtureTestCase() {
            state.fixture = new TableStoreWriteLockTest();
        }

        @Before
        public void startLockHolder() throws Exception {
            State run = state;
            run.fixture.setUp();
            Field directory = TableStoreWriteLockTest.class.getDeclaredField("tempDir");
            directory.setAccessible(true);
            run.directory = (Path) directory.get(run.fixture);
            run.lock = (TableStoreWriteLock) invoke(run.fixture, "createTestLock", new Class<?>[0]);
            run.child = (Process) invoke(run.fixture, "runLockingProcess", new Class<?>[]{boolean.class}, true);
            invoke(run.fixture, "waitForLockToBecomeLocked", new Class<?>[]{TableStoreWriteLock.class}, run.lock);
        }

        @After
        public void closeFixture() {
            state.fixture.tearDown();
        }
    }

    public static class FailureTestCase extends FixtureTestCase {
        @Test
        public void body() {
            throw state.originalFailure;
        }
    }

    public static class DiagnosticFailureTestCase extends FailureTestCase {
        @Rule
        public final TestWatcher diagnostics = state.fixture.failureDiagnostics;
    }

    public static class SuccessTestCase extends FixtureTestCase {
        @Test
        public void body() {
            assertTrue(state.lock.locked());
        }
    }

    public static class TimeoutTestCase extends FixtureTestCase {
        @Test(timeout = 200)
        public void body() {
            State run = state;
            run.bodyThread = Thread.currentThread();
            boolean interrupted = false;
            while (true) {
                try {
                    run.releaseBody.await();
                    break;
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    private static Object invoke(Object fixture, String name, Class<?>[] types, Object... arguments) throws Exception {
        Method method = TableStoreWriteLockTest.class.getDeclaredMethod(name, types);
        method.setAccessible(true);
        try {
            return method.invoke(fixture, arguments);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception)
                throw (Exception) cause;
            throw (Error) cause;
        }
    }

    private static class State {
        final AssertionError originalFailure = new AssertionError("Deliberate failure after the lock-holder starts");
        final CountDownLatch releaseBody = new CountDownLatch(1);
        TableStoreWriteLockTest fixture;
        TableStoreWriteLock lock;
        Process child;
        Path directory;
        volatile Thread bodyThread;
    }
}
