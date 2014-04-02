package net.openhft.chronicle.sandbox.queue;

import junit.framework.AssertionFailedError;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Base class for JSR166 Junit TCK tests.  Defines some constants,
 * utility methods and classes, as well as a simple framework for
 * helping to make sure that assertions Assert.failing in generated threads
 * cause the associated test that generated them to itself Assert.fail (which
 * JUnit does not otherwise arrange).  The rules for creating such
 * tests are:
 * <p/>
 * <ol>
 * <p/>
 * <li> All assertions in code running in generated threads must use
 * the forms {@link #threadFail}, {@link #threadAssertTrue}, {@link
 * #threadAssertEquals}, or {@link #threadAssertNull}, (not
 * {@code Assert.fail}, {@code Assert.assertTrue}, etc.) It is OK (but not
 * particularly recommended) for other code to use these forms too.
 * Only the most typically used JUnit assertion methods are defined
 * this way, but enough to live with.</li>
 * <p/>
 * <li> If you override {@link #setUp} or {@link #tearDown}, make sure
 * to invoke {@code super.setUp} and {@code super.tearDown} within
 * them. These methods are used to clear and check for thread
 * assertion Assert.failures.</li>
 * <p/>
 * <li>All delays and timeouts must use one of the constants {@code
 * SHORT_DELAY_MS}, {@code SMALL_DELAY_MS}, {@code MEDIUM_DELAY_MS},
 * {@code LONG_DELAY_MS}. The idea here is that a SHORT is always
 * discriminable from zero time, and always allows enough time for the
 * small amounts of computation (creating a thread, calling a few
 * methods, etc) needed to reach a timeout point. Similarly, a SMALL
 * is always discriminable as larger than SHORT and smaller than
 * MEDIUM.  And so on. These constants are set to conservative values,
 * but even so, if there is ever any doubt, they can all be increased
 * in one spot to rerun tests on slower platforms.</li>
 * <p/>
 * <li> All threads generated must be joined inside each test case
 * method (or {@code Assert.fail} to do so) before returning from the
 * method. The {@code joinPool} method can be used to do this when
 * using Executors.</li>
 * <p/>
 * </ol>
 * <p/>
 * <p><b>Other notes</b>
 * <ul>
 * <p/>
 * <li> Usually, there is one testcase method per JSR166 method
 * covering "normal" operation, and then as many exception-testing
 * methods as there are exceptions the method can throw. Sometimes
 * there are multiple tests per JSR166 method when the different
 * "normal" behaviors differ significantly. And sometimes testcases
 * cover multiple methods when they cannot be tested in
 * isolation.</li>
 * <p/>
 * <li> The documentation style for testcases is to provide as javadoc
 * a simple sentence or two describing the property that the testcase
 * method purports to test. The javadocs do not say anything about how
 * the property is tested. To find out, read the code.</li>
 * <p/>
 * <li> These tests are "conformance tests", and do not attempt to
 * test throughput, latency, scalability or other performance factors
 * (see the separate "jtreg" tests for a set intended to check these
 * for the most central aspects of functionality.) So, most tests use
 * the smallest sensible numbers of threads, collection sizes, etc
 * needed to check basic conformance.</li>
 * <p/>
 * <li>The test classes currently do not declare inclusion in
 * any particular package to simplify things for people integrating
 * them in TCK test suites.</li>
 * <p/>
 * <li> As a convenience, the {@code main} of this class (JSR166TestCase)
 * runs all JSR166 unit tests.</li>
 * <p/>
 * </ul>
 */
public class JSR166TestCase {

    /**
     * The number of elements to place in collections, arrays, etc.
     */
    public static final int SIZE = 20;
    public static final Integer zero = new Integer(0);
    public static final Integer one = new Integer(1);
    public static final Integer two = new Integer(2);
    public static final Integer three = new Integer(3);
    public static final Integer four = new Integer(4);
    public static final Integer five = new Integer(5);
    public static final Integer six = new Integer(6);
    public static final Integer seven = new Integer(7);
    public static final Integer eight = new Integer(8);
    public static final Integer nine = new Integer(9);
    public static final Integer m1 = new Integer(-1);
    public static final Integer m2 = new Integer(-2);
    public static final Integer m3 = new Integer(-3);
    public static final Integer m4 = new Integer(-4);
    public static final Integer m5 = new Integer(-5);
    public static final Integer m6 = new Integer(-6);
    public static final Integer m10 = new Integer(-10);
    public static final Integer notPresent = 42;
    public static final String TEST_STRING = "a test string";
    protected static final boolean expensiveTests = false;
    public static long SHORT_DELAY_MS;
    public static long SMALL_DELAY_MS;
    public static long MEDIUM_DELAY_MS;
    public static long LONG_DELAY_MS;
    /**
     * The first exception encountered if any threadAssertXXX method Assert.fails.
     */
    private final AtomicReference<Throwable> threadFailure
            = new AtomicReference<Throwable>(null);

    /**
     * Delays, via Thread.sleep, for the given millisecond delay, but
     * if the sleep is shorter than specified, may re-sleep or yield
     * until time elapses.
     */
    static void delay(long millis) throws InterruptedException {
        long startTime = System.nanoTime();
        long ns = millis * 1000 * 1000;
        for (; ; ) {
            if (millis > 0L)
                Thread.sleep(millis);
            else // too short to sleep
                Thread.yield();
            long d = ns - (System.nanoTime() - startTime);
            if (d > 0L)
                millis = d / (1000 * 1000);
            else
                break;
        }
    }

    /**
     * Returns a policy containing all the permissions we ever need.
     */
    public static Policy permissivePolicy() {
        return new AdjustablePolicy
                // Permissions j.u.c. needs directly
                (new RuntimePermission("modifyThread"),
                        new RuntimePermission("getClassLoader"),
                        new RuntimePermission("setContextClassLoader"),
                        // Permissions needed to change permissions!
                        new SecurityPermission("getPolicy"),
                        new SecurityPermission("setPolicy"),
                        new RuntimePermission("setSecurityManager"),
                        // Permissions needed by the junit test harness
                        new RuntimePermission("accessDeclaredMembers"),
                        new PropertyPermission("*", "read"),
                        new java.io.FilePermission("<<ALL FILES>>", "read"));
    }

    public static TrackedRunnable trackedRunnable(final long timeoutMillis) {
        return new TrackedRunnable() {
            private volatile boolean done = false;

            public boolean isDone() {
                return done;
            }

            public void run() {
                try {
                    delay(timeoutMillis);
                    done = true;
                } catch (InterruptedException ok) {
                }
            }
        };
    }

    /**
     * Returns the shortest timed delay. This could
     * be reimplemented to use for example a Property.
     */
    protected long getShortDelay() {
        return 50;
    }

    /**
     * Sets delays as multiples of SHORT_DELAY.
     */
    protected void setDelays() {
        SHORT_DELAY_MS = getShortDelay();
        SMALL_DELAY_MS = SHORT_DELAY_MS * 5;
        MEDIUM_DELAY_MS = SHORT_DELAY_MS * 10;
        LONG_DELAY_MS = SHORT_DELAY_MS * 200;
    }

    /**
     * Returns a timeout in milliseconds to be used in tests that
     * verify that operations block or time out.
     */
    protected long timeoutMillis() {
        return SHORT_DELAY_MS / 4;
    }

    /**
     * Returns a new Date instance representing a time delayMillis
     * milliseconds in the future.
     */
    Date delayedDate(long delayMillis) {
        return new Date(System.currentTimeMillis() + delayMillis);
    }

    /**
     * Records an exception so that it can be rethrown later in the test
     * harness thread, triggering a test case Assert.failure.  Only the first
     * Assert.failure is recorded; subsequent calls to this method from within
     * the same test have no effect.
     */
    public void threadRecordFailure(Throwable t) {
        threadFailure.compareAndSet(null, t);
    }

    @Before
    public void setUp() {
        setDelays();
    }

    // Some convenient Integer constants

    /**
     * Extra checks that get done for all test cases.
     * <p/>
     * Triggers test case Assert.failure if any thread assertions have Assert.failed,
     * by rethrowing, in the test harness thread, any exception recorded
     * earlier by threadRecordFailure.
     * <p/>
     * Triggers test case Assert.failure if interrupt status is set in the main thread.
     */
    @After
    public void tearDown() throws Exception {
        Throwable t = threadFailure.getAndSet(null);
        if (t != null) {
            if (t instanceof Error)
                throw (Error) t;
            else if (t instanceof RuntimeException)
                throw (RuntimeException) t;
            else if (t instanceof Exception)
                throw (Exception) t;
            else {
                AssertionFailedError afe =
                        new AssertionFailedError(t.toString());
                afe.initCause(t);
                throw afe;
            }
        }

        if (Thread.interrupted())
            throw new AssertionFailedError("interrupt status set in main thread");

        checkForkJoinPoolThreadLeaks();
    }

    /**
     * Find missing try { ... } finally { joinPool(e); }
     */
    void checkForkJoinPoolThreadLeaks() throws InterruptedException {
        Thread[] survivors = new Thread[5];
        int count = Thread.enumerate(survivors);
        for (int i = 0; i < count; i++) {
            Thread thread = survivors[i];
            String name = thread.getName();
            if (name.startsWith("ForkJoinPool-")) {
                // give thread some time to terminate
                thread.join(LONG_DELAY_MS);
                if (!thread.isAlive()) continue;
                thread.stop();
                throw new AssertionFailedError
                        (String.format("Found leaked ForkJoinPool thread test=%s thread=%s%n",
                                toString(), name));
            }
        }
    }

    /**
     * Just like Assert.fail(reason), but additionally recording (using
     * threadRecordFailure) any AssertionFailedError thrown, so that
     * the current testcase will Assert.fail.
     */
    public void threadFail(String reason) {
        try {
            Assert.fail(reason);
        } catch (AssertionFailedError t) {
            threadRecordFailure(t);
            Assert.fail(reason);
        }
    }

    /**
     * Just like Assert.assertTrue(b), but additionally recording (using
     * threadRecordFailure) any AssertionFailedError thrown, so that
     * the current testcase will Assert.fail.
     */
    public void threadAssertTrue(boolean b) {
        try {
            Assert.assertTrue(b);
        } catch (AssertionFailedError t) {
            threadRecordFailure(t);
            throw t;
        }
    }

    /**
     * Just like assertFalse(b), but additionally recording (using
     * threadRecordFailure) any AssertionFailedError thrown, so that
     * the current testcase will Assert.fail.
     */
    public void threadAssertFalse(boolean b) {
        try {
            Assert.assertFalse(b);
        } catch (AssertionFailedError t) {
            threadRecordFailure(t);
            throw t;
        }
    }

    /**
     * Just like assertNull(x), but additionally recording (using
     * threadRecordFailure) any AssertionFailedError thrown, so that
     * the current testcase will Assert.fail.
     */
    public void threadAssertNull(Object x) {
        try {
            Assert.assertNull(x);
        } catch (AssertionFailedError t) {
            threadRecordFailure(t);
            throw t;
        }
    }

    /**
     * Just like assertEquals(x, y), but additionally recording (using
     * threadRecordFailure) any AssertionFailedError thrown, so that
     * the current testcase will Assert.fail.
     */
    public void threadAssertEquals(long x, long y) {
        try {
            Assert.assertEquals(x, y);
        } catch (AssertionFailedError t) {
            threadRecordFailure(t);
            throw t;
        }
    }

    /**
     * Just like assertEquals(x, y), but additionally recording (using
     * threadRecordFailure) any AssertionFailedError thrown, so that
     * the current testcase will Assert.fail.
     */
    public void threadAssertEquals(Object x, Object y) {
        try {
            Assert.assertEquals(x, y);
        } catch (AssertionFailedError t) {
            threadRecordFailure(t);
            throw t;
        } catch (Throwable t) {
            threadUnexpectedException(t);
        }
    }

    /**
     * Just like assertSame(x, y), but additionally recording (using
     * threadRecordFailure) any AssertionFailedError thrown, so that
     * the current testcase will Assert.fail.
     */
    public void threadAssertSame(Object x, Object y) {
        try {
            Assert.assertSame(x, y);
        } catch (AssertionFailedError t) {
            threadRecordFailure(t);
            throw t;
        }
    }

    /**
     * Calls threadFail with message "should throw exception".
     */
    public void threadShouldThrow() {
        threadFail("should throw exception");
    }

    /**
     * Calls threadFail with message "should throw" + exceptionName.
     */
    public void threadShouldThrow(String exceptionName) {
        threadFail("should throw " + exceptionName);
    }

    /**
     * Records the given exception using {@link #threadRecordFailure},
     * then rethrows the exception, wrapping it in an
     * AssertionFailedError if necessary.
     */
    public void threadUnexpectedException(Throwable t) {
        threadRecordFailure(t);
        t.printStackTrace();
        if (t instanceof RuntimeException)
            throw (RuntimeException) t;
        else if (t instanceof Error)
            throw (Error) t;
        else {
            AssertionFailedError afe =
                    new AssertionFailedError("unexpected exception: " + t);
            afe.initCause(t);
            throw afe;
        }
    }

    /**
     * Waits out termination of a thread pool or Assert.fails doing so.
     */
    protected void joinPool(ExecutorService exec) {
        try {
            exec.shutdown();
            Assert.assertTrue("ExecutorService did not terminate in a timely manner",
                    exec.awaitTermination(2 * LONG_DELAY_MS, MILLISECONDS));
        } catch (SecurityException ok) {
            // Allowed in case test doesn't have privs
        } catch (InterruptedException ie) {
            Assert.fail("Unexpected InterruptedException");
        }
    }

    /**
     * Checks that thread does not terminate within the default
     * millisecond delay of {@code timeoutMillis()}.
     */
    protected void assertThreadStaysAlive(Thread thread) {
        assertThreadStaysAlive(thread, timeoutMillis());
    }

    /**
     * Checks that thread does not terminate within the given millisecond delay.
     */
    void assertThreadStaysAlive(Thread thread, long millis) {
        try {
            // No need to optimize the Assert.failing case via Thread.join.
            delay(millis);
            Assert.assertTrue(thread.isAlive());
        } catch (InterruptedException ie) {
            Assert.fail("Unexpected InterruptedException");
        }
    }

    /**
     * Checks that the threads do not terminate within the default
     * millisecond delay of {@code timeoutMillis()}.
     */
    void assertThreadsStayAlive(Thread... threads) {
        assertThreadsStayAlive(timeoutMillis(), threads);
    }

    /**
     * Checks that the threads do not terminate within the given millisecond delay.
     */
    void assertThreadsStayAlive(long millis, Thread... threads) {
        try {
            // No need to optimize the Assert.failing case via Thread.join.
            delay(millis);
            for (Thread thread : threads)
                Assert.assertTrue(thread.isAlive());
        } catch (InterruptedException ie) {
            Assert.fail("Unexpected InterruptedException");
        }
    }

    /**
     * Checks that future.get times out, with the default timeout of
     * {@code timeoutMillis()}.
     */
    void assertFutureTimesOut(Future future) {
        assertFutureTimesOut(future, timeoutMillis());
    }

    /**
     * Checks that future.get times out, with the given millisecond timeout.
     */
    void assertFutureTimesOut(Future future, long timeoutMillis) {
        long startTime = System.nanoTime();
        try {
            future.get(timeoutMillis, MILLISECONDS);
            shouldThrow();
        } catch (TimeoutException success) {
        } catch (Exception e) {
            threadUnexpectedException(e);
        } finally {
            future.cancel(true);
        }
        Assert.assertTrue(millisElapsedSince(startTime) >= timeoutMillis);
    }

    /**
     * Fails with message "should throw exception".
     */
    public void shouldThrow() {
        Assert.fail("Should throw exception");
    }

    /**
     * Fails with message "should throw " + exceptionName.
     */
    public void shouldThrow(String exceptionName) {
        Assert.fail("Should throw " + exceptionName);
    }

    /**
     * android-changed
     * Android does not use a SecurityManager. This will simply execute
     * the runnable ingoring permisions.
     */
    public void runWithPermissions(Runnable r, Permission... permissions) {
        r.run();
    }

    /**
     * android-changed
     * Android does not use a SecurityManager. This will simply execute
     * the runnable ingoring permisions.
     */
    public void runWithSecurityManagerWithPermissions(Runnable r,
                                                      Permission... permissions) {
        r.run();
    }

    /**
     * Runs a runnable without any permissions.
     */
    public void runWithoutPermissions(Runnable r) {
        runWithPermissions(r);
    }

    /**
     * Sleeps until the given time has elapsed.
     * Throws AssertionFailedError if interrupted.
     */
    void sleep(long millis) {
        try {
            delay(millis);
        } catch (InterruptedException ie) {
            AssertionFailedError afe =
                    new AssertionFailedError("Unexpected InterruptedException");
            afe.initCause(ie);
            throw afe;
        }
    }

    /**
     * Spin-waits up to the specified number of milliseconds for the given
     * thread to enter a wait state: BLOCKED, WAITING, or TIMED_WAITING.
     */
    protected void waitForThreadToEnterWaitState(Thread thread, long timeoutMillis) {
        long startTime = System.nanoTime();
        for (; ; ) {
            Thread.State s = thread.getState();
            if (s == Thread.State.BLOCKED ||
                    s == Thread.State.WAITING ||
                    s == Thread.State.TIMED_WAITING)
                return;
            else if (s == Thread.State.TERMINATED)
                Assert.fail("Unexpected thread termination");
            else if (millisElapsedSince(startTime) > timeoutMillis) {
                threadAssertTrue(thread.isAlive());
                return;
            }
            Thread.yield();
        }
    }

    /**
     * Waits up to LONG_DELAY_MS for the given thread to enter a wait
     * state: BLOCKED, WAITING, or TIMED_WAITING.
     */
    void waitForThreadToEnterWaitState(Thread thread) {
        waitForThreadToEnterWaitState(thread, LONG_DELAY_MS);
    }

    /**
     * Returns the number of milliseconds since time given by
     * startNanoTime, which must have been previously returned from a
     * call to.
     */
    protected long millisElapsedSince(long startNanoTime) {
        return NANOSECONDS.toMillis(System.nanoTime() - startNanoTime);
    }

    /**
     * Returns a new started daemon Thread running the given runnable.
     */
    protected Thread newStartedThread(Runnable runnable) {
        Thread t = new Thread(runnable);
        t.setDaemon(true);
        t.start();
        return t;
    }

    /**
     * Waits for the specified time (in milliseconds) for the thread
     * to terminate (using {@link Thread#join(long)}), else interrupts
     * the thread (in the hope that it may terminate later) and Assert.fails.
     */
    protected void awaitTermination(Thread t, long timeoutMillis) {
        try {
            t.join(timeoutMillis);
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        } finally {
            if (t.getState() != Thread.State.TERMINATED) {
                t.interrupt();
                Assert.fail("Test timed out");
            }
        }
    }

    // Some convenient Runnable classes

    /**
     * Waits for LONG_DELAY_MS milliseconds for the thread to
     * terminate (using {@link Thread#join(long)}), else interrupts
     * the thread (in the hope that it may terminate later) and Assert.fails.
     */
    protected void awaitTermination(Thread t) {
        awaitTermination(t, LONG_DELAY_MS);
    }

    public Callable<String> latchAwaitingStringTask(final CountDownLatch latch) {
        return new CheckedCallable<String>() {
            protected String realCall() {
                try {
                    latch.await();
                } catch (InterruptedException quittingTime) {
                }
                return TEST_STRING;
            }
        };
    }

    public Runnable awaiter(final CountDownLatch latch) {
        return new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                await(latch);
            }
        };
    }

    public void await(CountDownLatch latch) {
        try {
            Assert.assertTrue(latch.await(LONG_DELAY_MS, MILLISECONDS));
        } catch (Throwable t) {
            threadUnexpectedException(t);
        }
    }

    public void await(Semaphore semaphore) {
        try {
            Assert.assertTrue(semaphore.tryAcquire(LONG_DELAY_MS, MILLISECONDS));
        } catch (Throwable t) {
            threadUnexpectedException(t);
        }
    }

    public Runnable possiblyInterruptedRunnable(final long timeoutMillis) {
        return new CheckedRunnable() {
            protected void realRun() {
                try {
                    delay(timeoutMillis);
                } catch (InterruptedException ok) {
                }
            }
        };
    }

    protected void checkEmpty(BlockingQueue q) {
        try {
            Assert.assertTrue(q.isEmpty());
            Assert.assertEquals(0, q.size());
            Assert.assertNull(q.peek());
            Assert.assertNull(q.poll());
            Assert.assertNull(q.poll(0, MILLISECONDS));
            Assert.assertEquals(q.toString(), "[]");
            Assert.assertTrue(Arrays.equals(q.toArray(), new Object[0]));
            Assert.assertFalse(q.iterator().hasNext());
            try {
                q.element();
                shouldThrow();
            } catch (NoSuchElementException success) {
            }
            try {
                q.iterator().next();
                shouldThrow();
            } catch (NoSuchElementException success) {
            }
            try {
                q.remove();
                shouldThrow();
            } catch (NoSuchElementException success) {
            }
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        }
    }

    void assertSerialEquals(Object x, Object y) {
        Assert.assertTrue(Arrays.equals(serialBytes(x), serialBytes(y)));
    }

    void assertNotSerialEquals(Object x, Object y) {
        Assert.assertFalse(Arrays.equals(serialBytes(x), serialBytes(y)));
    }

    byte[] serialBytes(Object o) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(o);
            oos.flush();
            oos.close();
            return bos.toByteArray();
        } catch (Throwable t) {
            threadUnexpectedException(t);
            return new byte[0];
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> T serialClone(T o) {
        try {
            ObjectInputStream ois = new ObjectInputStream
                    (new ByteArrayInputStream(serialBytes(o)));
            T clone = (T) ois.readObject();
            Assert.assertSame(o.getClass(), clone.getClass());
            return clone;
        } catch (Throwable t) {
            threadUnexpectedException(t);
            return null;
        }
    }

    public void assertThrows(Class<? extends Throwable> expectedExceptionClass,
                             Runnable... throwingActions) {
        for (Runnable throwingAction : throwingActions) {
            boolean threw = false;
            try {
                throwingAction.run();
            } catch (Throwable t) {
                threw = true;
                if (!expectedExceptionClass.isInstance(t)) {
                    AssertionFailedError afe =
                            new AssertionFailedError
                                    ("Expected " + expectedExceptionClass.getName() +
                                            ", got " + t.getClass().getName());
                    afe.initCause(t);
                    threadUnexpectedException(afe);
                }
            }
            if (!threw)
                shouldThrow(expectedExceptionClass.getName());
        }
    }

    public interface TrackedRunnable extends Runnable {
        boolean isDone();
    }

    /**
     * A security policy where new permissions can be dynamically added
     * or all cleared.
     */
    public static class AdjustablePolicy extends Policy {
        Permissions perms = new Permissions();

        AdjustablePolicy(Permission... permissions) {
            for (Permission permission : permissions)
                perms.add(permission);
        }

        void addPermission(Permission perm) {
            perms.add(perm);
        }

        void clearPermissions() {
            perms = new Permissions();
        }

        public PermissionCollection getPermissions(CodeSource cs) {
            return perms;
        }

        public PermissionCollection getPermissions(ProtectionDomain pd) {
            return perms;
        }

        public boolean implies(ProtectionDomain pd, Permission p) {
            return perms.implies(p);
        }

        public void refresh() {
        }

        public String toString() {
            List<Permission> ps = new ArrayList<Permission>();
            for (Enumeration<Permission> e = perms.elements(); e.hasMoreElements(); )
                ps.add(e.nextElement());
            return "AdjustablePolicy with permissions " + ps;
        }
    }

//     /**
//      * Spin-waits up to LONG_DELAY_MS until flag becomes true.
//      */
//     public void await(AtomicBoolean flag) {
//         await(flag, LONG_DELAY_MS);
//     }

//     /**
//      * Spin-waits up to the specified timeout until flag becomes true.
//      */
//     public void await(AtomicBoolean flag, long timeoutMillis) {
//         long startTime = System.nanoTime();
//         while (!flag.get()) {
//             if (millisElapsedSince(startTime) > timeoutMillis)
//                 throw new AssertionFailedError("timed out");
//             Thread.yield();
//         }
//     }

    public static class NoOpRunnable implements Runnable {
        public void run() {
        }
    }

    public static class NoOpCallable implements Callable {
        public Object call() {
            return Boolean.TRUE;
        }
    }

    public static class StringTask implements Callable<String> {
        public String call() {
            return TEST_STRING;
        }
    }

    public static class NPETask implements Callable<String> {
        public String call() {
            throw new NullPointerException();
        }
    }

    public static class CallableOne implements Callable<Integer> {
        public Integer call() {
            return one;
        }
    }

    /**
     * For use as ThreadFactory in constructors
     */
    public static class SimpleThreadFactory implements ThreadFactory {
        public Thread newThread(Runnable r) {
            return new Thread(r);
        }
    }

    public static class TrackedShortRunnable implements Runnable {
        public volatile boolean done = false;

        public void run() {
            try {
                delay(SHORT_DELAY_MS);
                done = true;
            } catch (InterruptedException ok) {
            }
        }
    }

    public static class TrackedSmallRunnable implements Runnable {
        public volatile boolean done = false;

        public void run() {
            try {
                delay(SMALL_DELAY_MS);
                done = true;
            } catch (InterruptedException ok) {
            }
        }
    }

    public static class TrackedMediumRunnable implements Runnable {
        public volatile boolean done = false;

        public void run() {
            try {
                delay(MEDIUM_DELAY_MS);
                done = true;
            } catch (InterruptedException ok) {
            }
        }
    }

    public static class TrackedLongRunnable implements Runnable {
        public volatile boolean done = false;

        public void run() {
            try {
                delay(LONG_DELAY_MS);
                done = true;
            } catch (InterruptedException ok) {
            }
        }
    }

    public static class TrackedNoOpRunnable implements Runnable {
        public volatile boolean done = false;

        public void run() {
            done = true;
        }
    }

    public static class TrackedCallable implements Callable {
        public volatile boolean done = false;

        public Object call() {
            try {
                delay(SMALL_DELAY_MS);
                done = true;
            } catch (InterruptedException ok) {
            }
            return Boolean.TRUE;
        }
    }

    /**
     * For use as RejectedExecutionHandler in constructors
     */
    public static class NoOpREHandler implements RejectedExecutionHandler {
        public void rejectedExecution(Runnable r,
                                      ThreadPoolExecutor executor) {
        }
    }

    public abstract class CheckedRunnable implements Runnable {
        protected abstract void realRun() throws Throwable;

        public final void run() {
            try {
                realRun();
            } catch (Throwable t) {
                threadUnexpectedException(t);
            }
        }
    }

    public abstract class RunnableShouldThrow implements Runnable {
        final Class<?> exceptionClass;

        <T extends Throwable> RunnableShouldThrow(Class<T> exceptionClass) {
            this.exceptionClass = exceptionClass;
        }

        protected abstract void realRun() throws Throwable;

        public final void run() {
            try {
                realRun();
                threadShouldThrow(exceptionClass.getSimpleName());
            } catch (Throwable t) {
                if (!exceptionClass.isInstance(t))
                    threadUnexpectedException(t);
            }
        }
    }

    public abstract class ThreadShouldThrow extends Thread {
        final Class<?> exceptionClass;

        <T extends Throwable> ThreadShouldThrow(Class<T> exceptionClass) {
            this.exceptionClass = exceptionClass;
        }

        protected abstract void realRun() throws Throwable;

        public final void run() {
            try {
                realRun();
                threadShouldThrow(exceptionClass.getSimpleName());
            } catch (Throwable t) {
                if (!exceptionClass.isInstance(t))
                    threadUnexpectedException(t);
            }
        }
    }

    public abstract class CheckedInterruptedRunnable implements Runnable {
        protected abstract void realRun() throws Throwable;

        public final void run() {
            try {
                realRun();
                threadShouldThrow("InterruptedException");
            } catch (InterruptedException success) {
                threadAssertFalse(Thread.interrupted());
            } catch (Throwable t) {
                threadUnexpectedException(t);
            }
        }
    }

    public abstract class CheckedCallable<T> implements Callable<T> {
        protected abstract T realCall() throws Throwable;

        public final T call() {
            try {
                return realCall();
            } catch (Throwable t) {
                threadUnexpectedException(t);
                return null;
            }
        }
    }

    public abstract class CheckedInterruptedCallable<T>
            implements Callable<T> {
        protected abstract T realCall() throws Throwable;

        public final T call() {
            try {
                T result = realCall();
                threadShouldThrow("InterruptedException");
                return result;
            } catch (InterruptedException success) {
                threadAssertFalse(Thread.interrupted());
            } catch (Throwable t) {
                threadUnexpectedException(t);
            }
            return null;
        }
    }

    public class ShortRunnable extends CheckedRunnable {
        protected void realRun() throws Throwable {
            delay(SHORT_DELAY_MS);
        }
    }

    public class ShortInterruptedRunnable extends CheckedInterruptedRunnable {
        protected void realRun() throws InterruptedException {
            delay(SHORT_DELAY_MS);
        }
    }

    public class SmallRunnable extends CheckedRunnable {
        protected void realRun() throws Throwable {
            delay(SMALL_DELAY_MS);
        }
    }

    public class SmallPossiblyInterruptedRunnable extends CheckedRunnable {
        protected void realRun() {
            try {
                delay(SMALL_DELAY_MS);
            } catch (InterruptedException ok) {
            }
        }
    }

    public class SmallCallable extends CheckedCallable {
        protected Object realCall() throws InterruptedException {
            delay(SMALL_DELAY_MS);
            return Boolean.TRUE;
        }
    }

    public class MediumRunnable extends CheckedRunnable {
        protected void realRun() throws Throwable {
            delay(MEDIUM_DELAY_MS);
        }
    }

    public class MediumInterruptedRunnable extends CheckedInterruptedRunnable {
        protected void realRun() throws InterruptedException {
            delay(MEDIUM_DELAY_MS);
        }
    }

    public class MediumPossiblyInterruptedRunnable extends CheckedRunnable {
        protected void realRun() {
            try {
                delay(MEDIUM_DELAY_MS);
            } catch (InterruptedException ok) {
            }
        }
    }

    public class LongPossiblyInterruptedRunnable extends CheckedRunnable {
        protected void realRun() {
            try {
                delay(LONG_DELAY_MS);
            } catch (InterruptedException ok) {
            }
        }
    }

    /**
     * Analog of CheckedRunnable for RecursiveAction
     */
    public abstract class CheckedRecursiveAction extends RecursiveAction {
        protected abstract void realCompute() throws Throwable;

        @Override
        protected final void compute() {
            try {
                realCompute();
            } catch (Throwable t) {
                threadUnexpectedException(t);
            }
        }
    }

    /**
     * Analog of CheckedCallable for RecursiveTask
     */
    public abstract class CheckedRecursiveTask<T> extends RecursiveTask<T> {
        protected abstract T realCompute() throws Throwable;

        @Override
        protected final T compute() {
            try {
                return realCompute();
            } catch (Throwable t) {
                threadUnexpectedException(t);
                return null;
            }
        }
    }

    /**
     * A CyclicBarrier that uses timed await and Assert.fails with
     * AssertionFailedErrors instead of throwing checked exceptions.
     */
    public class CheckedBarrier extends CyclicBarrier {
        public CheckedBarrier(int parties) {
            super(parties);
        }

        public int await() {
            try {
                return super.await(2 * LONG_DELAY_MS, MILLISECONDS);
            } catch (TimeoutException e) {
                throw new AssertionFailedError("timed out");
            } catch (Exception e) {
                AssertionFailedError afe =
                        new AssertionFailedError("Unexpected exception: " + e);
                afe.initCause(e);
                throw afe;
            }
        }
    }
}

