package net.openhft.chronicle.sandbox.queue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.*;

/**
 * Copyright 2013 Peter Lawrey
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author Rob Austin
 */
public abstract class BlockingQueueTest extends JSR166TestCase {
    /*
     * This is the start of an attempt to refactor the tests for the
     * various related implementations of related interfaces without
     * too much duplicated code.  junit does not really support such
     * testing.  Here subclasses of TestCase not only contain tests,
     * but also configuration information that describes the
     * implementation class, most importantly how to instantiate
     * instances.
     */

    //----------------------------------------------------------------
    // Configuration methods
    //----------------------------------------------------------------

    /**
     * Returns an empty instance of the implementation class.
     */
    protected abstract BlockingQueue emptyCollection();

    /**
     * Returns an element suitable for insertion in the collection.
     * Override for collections with unusual element types.
     */
    protected Object makeElement(int i) {
        return Integer.valueOf(i);
    }

    //----------------------------------------------------------------
    // Tests
    //----------------------------------------------------------------

    /**
     * offer(null) throws NullPointerException
     */
    public void testOfferNull() {
        final Queue q = emptyCollection();
        try {
            q.offer(null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * add(null) throws NullPointerException
     */
    public void testAddNull() {
        final Collection q = emptyCollection();
        try {
            q.add(null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * timed offer(null) throws NullPointerException
     */
    public void testTimedOfferNull() throws InterruptedException {
        final BlockingQueue q = emptyCollection();
        long startTime = System.nanoTime();
        try {
            q.offer(null, LONG_DELAY_MS, MILLISECONDS);
            shouldThrow();
        } catch (NullPointerException success) {
        }
        assertTrue(millisElapsedSince(startTime) < LONG_DELAY_MS);
    }

    /**
     * put(null) throws NullPointerException
     */
    public void testPutNull() throws InterruptedException {
        final BlockingQueue q = emptyCollection();
        try {
            q.put(null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * put(null) throws NullPointerException
     */
    public void testAddAllNull() throws InterruptedException {
        final Collection q = emptyCollection();
        try {
            q.addAll(null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * addAll of a collection with null elements throws NullPointerException
     */
    public void testAddAllNullElements() {
        final Collection q = emptyCollection();
        final Collection<Integer> elements = Arrays.asList(new Integer[SIZE]);
        try {
            q.addAll(elements);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * toArray(null) throws NullPointerException
     */
    public void testToArray_NullArray() {
        final Collection q = emptyCollection();
        try {
            q.toArray(null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * drainTo(null) throws NullPointerException
     */
    public void testDrainToNull() {
        final BlockingQueue q = emptyCollection();
        try {
            q.drainTo(null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * drainTo(this) throws IllegalArgumentException
     */
    public void testDrainToSelf() {
        final BlockingQueue q = emptyCollection();
        try {
            q.drainTo(q);
            shouldThrow();
        } catch (IllegalArgumentException success) {
        }
    }

    /**
     * drainTo(null, n) throws NullPointerException
     */
    public void testDrainToNullN() {
        final BlockingQueue q = emptyCollection();
        try {
            q.drainTo(null, 0);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * drainTo(this, n) throws IllegalArgumentException
     */
    public void testDrainToSelfN() {
        final BlockingQueue q = emptyCollection();
        try {
            q.drainTo(q, 0);
            shouldThrow();
        } catch (IllegalArgumentException success) {
        }
    }

    /**
     * drainTo(c, n) returns 0 and does nothing when n <= 0
     */
    public void testDrainToNonPositiveMaxElements() {
        final BlockingQueue q = emptyCollection();
        final int[] ns = {0, -1, -42, Integer.MIN_VALUE};
        for (int n : ns)
            assertEquals(0, q.drainTo(new ArrayList(), n));
        if (q.remainingCapacity() > 0) {
            // Not SynchronousQueue, that is
            Object one = makeElement(1);
            q.add(one);
            ArrayList c = new ArrayList();
            for (int n : ns)
                assertEquals(0, q.drainTo(new ArrayList(), n));
            assertEquals(1, q.size());
            assertSame(one, q.poll());
            assertTrue(c.isEmpty());
        }
    }

    /**
     * timed poll before a delayed offer times out; after offer succeeds;
     * on interruption throws
     */
    public void testTimedPollWithOffer() throws InterruptedException {
        final BlockingQueue q = emptyCollection();
        final CheckedBarrier barrier = new CheckedBarrier(2);
        final Object zero = makeElement(0);
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                long startTime = System.nanoTime();
                assertNull(q.poll(timeoutMillis(), MILLISECONDS));
                assertTrue(millisElapsedSince(startTime) >= timeoutMillis());

                barrier.await();

                assertSame(zero, q.poll(LONG_DELAY_MS, MILLISECONDS));

                Thread.currentThread().interrupt();
                try {
                    q.poll(LONG_DELAY_MS, MILLISECONDS);
                    shouldThrow();
                } catch (InterruptedException success) {
                }
                assertFalse(Thread.interrupted());

                barrier.await();
                try {
                    q.poll(LONG_DELAY_MS, MILLISECONDS);
                    shouldThrow();
                } catch (InterruptedException success) {
                }
                assertFalse(Thread.interrupted());
            }
        });

        barrier.await();
        long startTime = System.nanoTime();
        assertTrue(q.offer(zero, LONG_DELAY_MS, MILLISECONDS));
        assertTrue(millisElapsedSince(startTime) < LONG_DELAY_MS);

        barrier.await();
        assertThreadStaysAlive(t);
        t.interrupt();
        awaitTermination(t);
    }

    /**
     * take() blocks interruptibly when empty
     */
    public void testTakeFromEmptyBlocksInterruptibly() {
        final BlockingQueue q = emptyCollection();
        final CountDownLatch threadStarted = new CountDownLatch(1);
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                threadStarted.countDown();
                try {
                    q.take();
                    shouldThrow();
                } catch (InterruptedException success) {
                }
                assertFalse(Thread.interrupted());
            }
        });

        await(threadStarted);
        assertThreadStaysAlive(t);
        t.interrupt();
        awaitTermination(t);
    }

    /**
     * take() throws InterruptedException immediately if interrupted
     * before waiting
     */
    public void testTakeFromEmptyAfterInterrupt() {
        final BlockingQueue q = emptyCollection();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                Thread.currentThread().interrupt();
                try {
                    q.take();
                    shouldThrow();
                } catch (InterruptedException success) {
                }
                assertFalse(Thread.interrupted());
            }
        });

        awaitTermination(t);
    }

    /**
     * timed poll() blocks interruptibly when empty
     */
    public void testTimedPollFromEmptyBlocksInterruptibly() {
        final BlockingQueue q = emptyCollection();
        final CountDownLatch threadStarted = new CountDownLatch(1);
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                threadStarted.countDown();
                try {
                    q.poll(2 * LONG_DELAY_MS, MILLISECONDS);
                    shouldThrow();
                } catch (InterruptedException success) {
                }
                assertFalse(Thread.interrupted());
            }
        });

        await(threadStarted);
        assertThreadStaysAlive(t);
        t.interrupt();
        awaitTermination(t);
    }

    /**
     * timed poll() throws InterruptedException immediately if
     * interrupted before waiting
     */
    public void testTimedPollFromEmptyAfterInterrupt() {
        final BlockingQueue q = emptyCollection();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                Thread.currentThread().interrupt();
                try {
                    q.poll(2 * LONG_DELAY_MS, MILLISECONDS);
                    shouldThrow();
                } catch (InterruptedException success) {
                }
                assertFalse(Thread.interrupted());
            }
        });

        awaitTermination(t);
    }

    /**
     * remove(x) removes x and returns true if present
     * TODO: move to superclass CollectionTest.java
     */
    public void testRemoveElement() {
        final BlockingQueue q = emptyCollection();
        final int size = Math.min(q.remainingCapacity(), SIZE);
        final Object[] elts = new Object[size];
        assertFalse(q.contains(makeElement(99)));
        assertFalse(q.remove(makeElement(99)));
        checkEmpty(q);
        for (int i = 0; i < size; i++)
            q.add(elts[i] = makeElement(i));
        for (int i = 1; i < size; i += 2) {
            for (int pass = 0; pass < 2; pass++) {
                assertEquals((pass == 0), q.contains(elts[i]));
                assertEquals((pass == 0), q.remove(elts[i]));
                assertFalse(q.contains(elts[i]));
                assertTrue(q.contains(elts[i - 1]));
                if (i < size - 1)
                    assertTrue(q.contains(elts[i + 1]));
            }
        }
        if (size > 0)
            assertTrue(q.contains(elts[0]));
        for (int i = size - 2; i >= 0; i -= 2) {
            assertTrue(q.contains(elts[i]));
            assertFalse(q.contains(elts[i + 1]));
            assertTrue(q.remove(elts[i]));
            assertFalse(q.contains(elts[i]));
            assertFalse(q.remove(elts[i + 1]));
            assertFalse(q.contains(elts[i + 1]));
        }
        checkEmpty(q);
    }

    /**
     * For debugging.
     */
    public void XXXXtestFails() {
        fail(emptyCollection().getClass().toString());
    }

}
