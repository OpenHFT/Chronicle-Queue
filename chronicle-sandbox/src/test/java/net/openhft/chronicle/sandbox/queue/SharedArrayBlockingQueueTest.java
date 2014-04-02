package net.openhft.chronicle.sandbox.queue;


import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.*;

public class SharedArrayBlockingQueueTest extends JSR166TestCase {

    /**
     * Returns a new queue of given size containing consecutive
     * Integers 0 ... n.
     */
    private ConcurrentSharedBlockingObjectQueue<Integer> populatedQueue(int n) {
        ConcurrentSharedBlockingObjectQueue<Integer> q = new ConcurrentSharedBlockingObjectQueue<Integer>(n);
        assertTrue(q.isEmpty());
        for (int i = 0; i < n; i++)
            assertTrue(q.offer(new Integer(i)));
        assertFalse(q.isEmpty());
        assertEquals(0, q.remainingCapacity());
        assertEquals(n, q.size());
        return q;
    }

    /**
     * A new queue has the indicated capacity
     */

    @Test
    public void testConstructor1() {
        assertEquals(SIZE, new ConcurrentSharedBlockingObjectQueue(SIZE).remainingCapacity());
    }

    /**
     * Constructor throws IAE if capacity argument nonpositive
     */
    @Test
    public void testConstructor2() {
        try {
            new ConcurrentSharedBlockingObjectQueue(0);
            shouldThrow();
        } catch (IllegalArgumentException success) {
        }
    }

    /**
     * Initializing from null Collection throws NPE
     */
    @Ignore
    @Test
    public void testConstructor3() {
        try {
            new ConcurrentSharedBlockingObjectQueue(1, true, null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * Initializing from Collection of null elements throws NPE
     */
    @Ignore
    @Test
    public void testConstructor4() {
        Collection<Integer> elements = Arrays.asList(new Integer[SIZE]);
        try {
            new ConcurrentSharedBlockingObjectQueue(SIZE, false, elements);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * Initializing from Collection with some null elements throws NPE
     */
    @Ignore
    @Test
    public void testConstructor5() {
        Integer[] ints = new Integer[SIZE];
        for (int i = 0; i < SIZE - 1; ++i)
            ints[i] = i;
        Collection<Integer> elements = Arrays.asList(ints);
        try {
            new ConcurrentSharedBlockingObjectQueue(SIZE, false, Arrays.asList(ints));
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * Initializing from too large collection throws IAE
     */
    @Ignore
    @Test
    public void testConstructor6() {
        Integer[] ints = new Integer[SIZE];
        for (int i = 0; i < SIZE; ++i)
            ints[i] = i;
        Collection<Integer> elements = Arrays.asList(ints);
        try {
            new ConcurrentSharedBlockingObjectQueue(SIZE - 1, false, elements);
            shouldThrow();
        } catch (IllegalArgumentException success) {
        }
    }

    /**
     * Queue contains all elements of collection used to initialize
     */
    @Ignore
    @Test
    public void testConstructor7() {
        Integer[] ints = new Integer[SIZE];
        for (int i = 0; i < SIZE; ++i)
            ints[i] = i;
        Collection<Integer> elements = Arrays.asList(ints);
        ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(SIZE, true, elements);
        for (int i = 0; i < SIZE; ++i)
            assertEquals(ints[i], q.poll());
    }

    /**
     * Queue transitions from empty to full when elements added
     */
    @Test
    public void testEmptyFull() {
        ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(2);
        assertTrue(q.isEmpty());
        assertEquals(2, q.remainingCapacity());
        q.add(one);
        assertFalse(q.isEmpty());
        q.add(two);
        assertFalse(q.isEmpty());
        assertEquals(0, q.remainingCapacity());
        assertFalse(q.offer(three));
    }

    /**
     * remainingCapacity decreases on add, increases on remove
     */
    @Test
    public void testRemainingCapacity() {
        ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.remainingCapacity());
            assertEquals(SIZE - i, q.size());
            q.remove();
        }
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(SIZE - i, q.remainingCapacity());
            assertEquals(i, q.size());
            q.add(new Integer(i));
        }
    }

    /**
     * Offer succeeds if not full; fails if full
     */
    @Test
    public void testOffer() {
        ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(1);
        assertTrue(q.offer(zero));
        assertFalse(q.offer(one));
    }

    /**
     * add succeeds if not full; throws ISE if full
     */
    @Test
    public void testAdd() {
        try {
            ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(SIZE);
            for (int i = 0; i < SIZE; ++i) {
                assertTrue(q.add(new Integer(i)));
            }
            assertEquals(0, q.remainingCapacity());
            q.add(new Integer(SIZE));
            shouldThrow();
        } catch (IllegalStateException success) {
        }
    }

    /**
     * addAll(this) throws IAE
     */
    @Test
    public void testAddAllSelf() {
        try {
            ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
            q.addAll(q);
            shouldThrow();
        } catch (IllegalArgumentException success) {
        }
    }

    /**
     * addAll of a collection with any null elements throws NPE after
     * possibly adding some elements
     */
    @Test
    public void testAddAll3() {
        try {
            ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(SIZE);
            Integer[] ints = new Integer[SIZE];
            for (int i = 0; i < SIZE - 1; ++i)
                ints[i] = new Integer(i);
            q.addAll(Arrays.asList(ints));
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * addAll throws ISE if not enough room
     */
    @Test
    public void testAddAll4() {
        try {
            ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(1);
            Integer[] ints = new Integer[SIZE];
            for (int i = 0; i < SIZE; ++i)
                ints[i] = new Integer(i);
            q.addAll(Arrays.asList(ints));
            shouldThrow();
        } catch (IllegalStateException success) {
        }
    }

    /**
     * Queue contains all elements, in traversal order, of successful addAll
     */
    @Test
    public void testAddAll5() {
        Integer[] empty = new Integer[0];
        Integer[] ints = new Integer[SIZE];
        for (int i = 0; i < SIZE; ++i)
            ints[i] = new Integer(i);
        ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(SIZE);
        assertFalse(q.addAll(Arrays.asList(empty)));
        assertTrue(q.addAll(Arrays.asList(ints)));
        for (int i = 0; i < SIZE; ++i)
            assertEquals(ints[i], q.poll());
    }

    /**
     * all elements successfully put are contained
     */
    @Test
    public void testPut() throws InterruptedException {
        ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            Integer I = new Integer(i);
            q.put(I);
            assertTrue(q.contains(I));
        }
        assertEquals(0, q.remainingCapacity());
    }

    /**
     * put blocks interruptibly if full
     */
    @Test
    public void testBlockingPut() throws InterruptedException {
        final ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(SIZE);
        final CountDownLatch pleaseInterrupt = new CountDownLatch(1);
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                for (int i = 0; i < SIZE; ++i)
                    q.put(i);
                assertEquals(SIZE, q.size());
                assertEquals(0, q.remainingCapacity());

                Thread.currentThread().interrupt();
                try {
                    q.put(99);
                    shouldThrow();
                } catch (InterruptedException success) {
                }
                assertFalse(Thread.interrupted());

                pleaseInterrupt.countDown();
                try {
                    q.put(99);
                    shouldThrow();
                } catch (InterruptedException success) {
                }
                assertFalse(Thread.interrupted());
            }
        });

        await(pleaseInterrupt);
        assertThreadStaysAlive(t);
        t.interrupt();
        awaitTermination(t);
        assertEquals(SIZE, q.size());
        assertEquals(0, q.remainingCapacity());
    }

    /**
     * put blocks interruptibly waiting for take when full
     */
    @Test
    public void testPutWithTake() throws InterruptedException {
        final int capacity = 2;
        final ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(capacity);
        final CountDownLatch pleaseTake = new CountDownLatch(1);
        final CountDownLatch pleaseInterrupt = new CountDownLatch(1);
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                for (int i = 0; i < capacity; i++)
                    q.put(i);
                pleaseTake.countDown();
                q.put(86);

                pleaseInterrupt.countDown();
                try {
                    q.put(99);
                    shouldThrow();
                } catch (InterruptedException success) {
                }
                assertFalse(Thread.interrupted());
            }
        });

        await(pleaseTake);
        assertEquals(0, q.remainingCapacity());
        assertEquals(0, q.take());

        await(pleaseInterrupt);
        assertThreadStaysAlive(t);
        t.interrupt();
        awaitTermination(t);
        assertEquals(0, q.remainingCapacity());
    }

    /**
     * timed offer times out if full and elements not taken
     */
    @Ignore
    @Test
    public void testTimedOffer() throws InterruptedException {
        final ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(2);
        final CountDownLatch pleaseInterrupt = new CountDownLatch(1);
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                q.put(new Object());
                q.put(new Object());
                long startTime = System.nanoTime();
                assertFalse(q.offer(new Object(), timeoutMillis(), MILLISECONDS));
                assertTrue(millisElapsedSince(startTime) >= timeoutMillis());
                pleaseInterrupt.countDown();
                try {
                    q.offer(new Object(), 2 * LONG_DELAY_MS, MILLISECONDS);
                    shouldThrow();
                } catch (InterruptedException success) {
                }
            }
        });

        await(pleaseInterrupt);
        assertThreadStaysAlive(t);
        t.interrupt();
        awaitTermination(t);
    }

    /**
     * take retrieves elements in FIFO order
     */
    @Test
    public void testTake() throws InterruptedException {
        ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.take());
        }
    }

    /**
     * Take removes existing elements until empty, then blocks interruptibly
     */
    @Ignore
    @Test
    public void testBlockingTake() throws InterruptedException {
        final ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        final CountDownLatch pleaseInterrupt = new CountDownLatch(1);
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                for (int i = 0; i < SIZE; ++i) {
                    assertEquals(i, q.take());
                }

                Thread.currentThread().interrupt();
                try {
                    q.take();
                    shouldThrow();
                } catch (InterruptedException success) {
                }
                assertFalse(Thread.interrupted());

                pleaseInterrupt.countDown();
                try {
                    q.take();
                    shouldThrow();
                } catch (InterruptedException success) {
                }
                assertFalse(Thread.interrupted());
            }
        });

        await(pleaseInterrupt);
        assertThreadStaysAlive(t);
        t.interrupt();
        awaitTermination(t);
    }

    /**
     * poll succeeds unless empty
     */
    @Test
    public void testPoll() {
        ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.poll());
        }
        assertNull(q.poll());
    }

    /**
     * timed poll with zero timeout succeeds when non-empty, else times out
     */
    @Test
    public void testTimedPoll0() throws InterruptedException {
        ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.poll(0, MILLISECONDS));
        }
        assertNull(q.poll(0, MILLISECONDS));
        checkEmpty(q);
    }

    /**
     * timed poll with nonzero timeout succeeds when non-empty, else times out
     */
    @Test
    public void testTimedPoll() throws InterruptedException {
        ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            long startTime = System.nanoTime();
            assertEquals(i, q.poll(LONG_DELAY_MS, MILLISECONDS));
            assertTrue(millisElapsedSince(startTime) < LONG_DELAY_MS);
        }
        long startTime = System.nanoTime();
        assertNull(q.poll(timeoutMillis(), MILLISECONDS));
        assertTrue(millisElapsedSince(startTime) >= timeoutMillis());
        checkEmpty(q);
    }

    /**
     * Interrupted timed poll throws InterruptedException instead of
     * returning timeout status
     */
    @Ignore
    @Test
    public void testInterruptedTimedPoll() throws InterruptedException {
        final BlockingQueue<Integer> q = populatedQueue(SIZE);
        final CountDownLatch aboutToWait = new CountDownLatch(1);
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                for (int i = 0; i < SIZE; ++i) {
                    long t0 = System.nanoTime();
                    assertEquals(i, (int) q.poll(LONG_DELAY_MS, MILLISECONDS));
                    assertTrue(millisElapsedSince(t0) < SMALL_DELAY_MS);
                }
                long t0 = System.nanoTime();
                aboutToWait.countDown();
                try {
                    q.poll(MEDIUM_DELAY_MS, MILLISECONDS);
                    shouldThrow();
                } catch (InterruptedException success) {
                    assertTrue(millisElapsedSince(t0) < MEDIUM_DELAY_MS);
                }
            }
        });

        aboutToWait.await();
        waitForThreadToEnterWaitState(t, SMALL_DELAY_MS);
        t.interrupt();
        awaitTermination(t, MEDIUM_DELAY_MS);
        checkEmpty(q);
    }

    /**
     * peek returns next element, or null if empty
     */
    @Test
    public void testPeek() {
        ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.peek());
            assertEquals(i, q.poll());
            assertTrue(q.peek() == null ||
                    !q.peek().equals(i));
        }
        assertNull(q.peek());
    }

    /**
     * element returns next element, or throws NSEE if empty
     */
    @Ignore
    @Test
    public void testElement() {
        ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.element());
            assertEquals(i, q.poll());
        }
        try {
            q.element();
            shouldThrow();
        } catch (NoSuchElementException success) {
        }
    }

    /**
     * remove removes next element, or throws NSEE if empty
     */
    @Test
    public void testRemove() {
        ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.remove());
        }
        try {
            q.remove();
            shouldThrow();
        } catch (NoSuchElementException success) {
        }
    }

    /**
     * contains(x) reports true when elements added but not yet removed
     */
    @Test
    public void testContains() {
        ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertTrue(q.contains(new Integer(i)));
            assertEquals(i, q.poll());
            assertFalse(q.contains(new Integer(i)));
        }
    }

    /**
     * clear removes all elements
     */
    @Test
    public void testClear() {
        ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        q.clear();
        assertTrue(q.isEmpty());
        assertEquals(0, q.size());
        assertEquals(SIZE, q.remainingCapacity());
        q.add(one);
        assertFalse(q.isEmpty());
        assertTrue(q.contains(one));
        q.clear();
        assertTrue(q.isEmpty());
    }

    /**
     * containsAll(c) is true when c contains a subset of elements
     */
    @Test
    public void testContainsAll() {
        ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        ConcurrentSharedBlockingObjectQueue p = new ConcurrentSharedBlockingObjectQueue(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertTrue(q.containsAll(p));
            assertFalse(p.containsAll(q));
            p.add(new Integer(i));
        }
        assertTrue(p.containsAll(q));
    }

    /**
     * retainAll(c) retains only those elements of c and reports true if changed
     */
    @Test
    public void testRetainAll() {
        ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        ConcurrentSharedBlockingObjectQueue p = populatedQueue(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            boolean changed = q.retainAll(p);
            if (i == 0)
                assertFalse(changed);
            else
                assertTrue(changed);

            assertTrue(q.containsAll(p));
            assertEquals(SIZE - i, q.size());
            p.remove();
        }
    }

    /**
     * removeAll(c) removes only those elements of c and reports true if changed
     */
    @Test
    public void testRemoveAll() {
        for (int i = 1; i < SIZE; ++i) {
            ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
            ConcurrentSharedBlockingObjectQueue p = populatedQueue(i);
            assertTrue(q.removeAll(p));
            assertEquals(SIZE - i, q.size());
            for (int j = 0; j < i; ++j) {
                Integer I = (Integer) (p.remove());
                assertFalse(q.contains(I));
            }
        }
    }

    void checkToArray(ConcurrentSharedBlockingObjectQueue q) {
        int size = q.size();
        Object[] o = q.toArray();
        assertEquals(size, o.length);
        Iterator it = q.iterator();
        for (int i = 0; i < size; i++) {
            Integer x = (Integer) it.next();
            assertEquals((Integer) o[0] + i, (int) x);
            assertSame(o[i], x);
        }
    }

    /**
     * toArray() contains all elements in FIFO order
     */
    @Test
    public void testToArray() {
        ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(SIZE);
        for (int i = 0; i < SIZE; i++) {
            checkToArray(q);
            q.add(i);
        }
        // Provoke wraparound
        for (int i = 0; i < SIZE; i++) {
            checkToArray(q);
            assertEquals(i, q.poll());
            checkToArray(q);
            q.add(SIZE + i);
        }
        for (int i = 0; i < SIZE; i++) {
            checkToArray(q);
            assertEquals(SIZE + i, q.poll());
        }
    }

    void checkToArray2(ConcurrentSharedBlockingObjectQueue q) {
        int size = q.size();
        Integer[] a1 = size == 0 ? null : new Integer[size - 1];
        Integer[] a2 = new Integer[size];
        Integer[] a3 = new Integer[size + 2];
        if (size > 0) Arrays.fill(a1, 42);
        Arrays.fill(a2, 42);
        Arrays.fill(a3, 42);
        Integer[] b1 = size == 0 ? null : (Integer[]) q.toArray(a1);
        Integer[] b2 = (Integer[]) q.toArray(a2);
        Integer[] b3 = (Integer[]) q.toArray(a3);
        assertSame(a2, b2);
        assertSame(a3, b3);
        Iterator it = q.iterator();
        for (int i = 0; i < size; i++) {
            Integer x = (Integer) it.next();
            assertSame(b1[i], x);
            assertEquals(b1[0] + i, (int) x);
            assertSame(b2[i], x);
            assertSame(b3[i], x);
        }
        assertNull(a3[size]);
        assertEquals(42, (int) a3[size + 1]);
        if (size > 0) {
            assertNotSame(a1, b1);
            assertEquals(size, b1.length);
            for (int i = 0; i < a1.length; i++) {
                assertEquals(42, (int) a1[i]);
            }
        }
    }

    /**
     * toArray(a) contains all elements in FIFO order
     */
    @Ignore
    @Test
    public void testToArray2() {
        ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(SIZE);
        for (int i = 0; i < SIZE; i++) {
            checkToArray2(q);
            q.add(i);
        }
        // Provoke wraparound
        for (int i = 0; i < SIZE; i++) {
            checkToArray2(q);
            assertEquals(i, q.poll());
            checkToArray2(q);
            q.add(SIZE + i);
        }
        for (int i = 0; i < SIZE; i++) {
            checkToArray2(q);
            assertEquals(SIZE + i, q.poll());
        }
    }

    /**
     * toArray(incompatible array type) throws ArrayStoreException
     */
    @Test
    public void testToArray1_BadArg() {
        ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        try {
            q.toArray(new String[10]);
            shouldThrow();
        } catch (ArrayStoreException success) {
        }
    }

    /**
     * iterator iterates through all elements
     */
    @Test
    public void testIterator() throws InterruptedException {
        ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        Iterator it = q.iterator();
        while (it.hasNext()) {
            assertEquals(it.next(), q.take());
        }
    }

    /**
     * iterator.remove removes current element
     */
    @Ignore
    @Test
    public void testIteratorRemove() {
        final ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(3);
        q.add(two);
        q.add(one);
        q.add(three);

        Iterator it = q.iterator();
        it.next();
        it.remove();

        it = q.iterator();
        assertSame(it.next(), one);
        assertSame(it.next(), three);
        assertFalse(it.hasNext());
    }

    /**
     * iterator ordering is FIFO
     */
    @Test
    public void testIteratorOrdering() {
        final ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(3);
        q.add(one);
        q.add(two);
        q.add(three);

        assertEquals("queue should be full", 0, q.remainingCapacity());

        int k = 0;
        for (Iterator it = q.iterator(); it.hasNext(); ) {
            assertEquals(++k, it.next());
        }
        assertEquals(3, k);
    }

    /**
     * Modifications do not cause iterators to fail
     */
    @Test
    public void testWeaklyConsistentIteration() {
        final ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(3);
        q.add(one);
        q.add(two);
        q.add(three);
        for (Iterator it = q.iterator(); it.hasNext(); ) {
            q.remove();
            it.next();
        }
        assertEquals(0, q.size());
    }

    /**
     * toString contains toStrings of elements
     */
    @Test
    public void testToString() {
        ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        String s = q.toString();
        for (int i = 0; i < SIZE; ++i) {
            assertTrue(s.contains(String.valueOf(i)));
        }
    }

    /**
     * offer transfers elements across Executor tasks
     */
    @Test
    public void testOfferInExecutor() {
        final ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(2);
        q.add(one);
        q.add(two);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        final CheckedBarrier threadsStarted = new CheckedBarrier(2);
        executor.execute(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                assertFalse(q.offer(three));
                threadsStarted.await();
                assertTrue(q.offer(three, LONG_DELAY_MS, MILLISECONDS));
                assertEquals(0, q.remainingCapacity());
            }
        });

        executor.execute(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                threadsStarted.await();
                assertEquals(0, q.remainingCapacity());
                assertSame(one, q.take());
            }
        });

        joinPool(executor);
    }

    /**
     * timed poll retrieves elements across Executor threads
     */
    @Test
    public void testPollInExecutor() {
        final ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(2);
        final CheckedBarrier threadsStarted = new CheckedBarrier(2);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.execute(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                assertNull(q.poll());
                threadsStarted.await();
                assertSame(one, q.poll(LONG_DELAY_MS, MILLISECONDS));
                checkEmpty(q);
            }
        });

        executor.execute(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                threadsStarted.await();
                q.put(one);
            }
        });

        joinPool(executor);
    }

    /**
     * A deserialized serialized queue has same elements in same order
     */
    @Ignore
    @Test
    public void testSerialization() throws Exception {
        Queue x = populatedQueue(SIZE);
        Queue y = serialClone(x);

        assertNotSame(x, y);
        assertEquals(x.size(), y.size());
        assertEquals(x.toString(), y.toString());
        assertTrue(Arrays.equals(x.toArray(), y.toArray()));
        while (!x.isEmpty()) {
            assertFalse(y.isEmpty());
            assertEquals(x.remove(), y.remove());
        }
        assertTrue(y.isEmpty());
    }

    /**
     * drainTo(c) empties queue into another collection c
     */
    @Test
    @Ignore
    public void testDrainTo() {
        ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        ArrayList l = new ArrayList();
        q.drainTo(l);
        assertEquals(0, q.size());
        assertEquals(SIZE, l.size());
        for (int i = 0; i < SIZE; ++i)
            assertEquals(l.get(i), new Integer(i));
        q.add(zero);
        q.add(one);
        assertFalse(q.isEmpty());
        assertTrue(q.contains(zero));
        assertTrue(q.contains(one));
        l.clear();
        q.drainTo(l);
        assertEquals(0, q.size());
        assertEquals(2, l.size());
        for (int i = 0; i < 2; ++i)
            assertEquals(l.get(i), new Integer(i));
    }

    /**
     * drainTo empties full queue, unblocking a waiting put.
     */
    @Ignore
    @Test
    public void testDrainToWithActivePut() throws InterruptedException {
        final ConcurrentSharedBlockingObjectQueue q = populatedQueue(SIZE);
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                q.put(new Integer(SIZE + 1));
            }
        });

        t.start();
        ArrayList l = new ArrayList();
        q.drainTo(l);
        assertTrue(l.size() >= SIZE);
        for (int i = 0; i < SIZE; ++i)
            assertEquals(l.get(i), new Integer(i));
        t.join();
        assertTrue(q.size() + l.size() >= SIZE);
    }

    /**
     * drainTo(c, n) empties first min(n, size) elements of queue into c
     */
    @Ignore
    @Test
    public void testDrainToN() {
        ConcurrentSharedBlockingObjectQueue q = new ConcurrentSharedBlockingObjectQueue(SIZE * 2);
        for (int i = 0; i < SIZE + 2; ++i) {
            for (int j = 0; j < SIZE; j++)
                assertTrue(q.offer(new Integer(j)));
            ArrayList l = new ArrayList();
            q.drainTo(l, i);
            int k = (i < SIZE) ? i : SIZE;
            assertEquals(k, l.size());
            assertEquals(SIZE - k, q.size());
            for (int j = 0; j < k; ++j)
                assertEquals(l.get(j), new Integer(j));
            while (q.poll() != null) ;
        }
    }

    public static class Fair extends BlockingQueueTest {
        protected BlockingQueue emptyCollection() {
            return new ConcurrentSharedBlockingObjectQueue(SIZE, true);
        }
    }

    public static class NonFair extends BlockingQueueTest {
        protected BlockingQueue emptyCollection() {
            return new ConcurrentSharedBlockingObjectQueue(SIZE, false);
        }
    }

}

