package net.openhft.chronicle.sandbox.queue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A low latency, lock free, Object bounded blocking queue backed by an Object[].
 * Unlike the other classes in this package thi class implements {@linkplain java.util.concurrent.BlockingQueue BlockingQueue},
 * <p/>
 * This class takes advantage of the Unsafe.putOrderedObject, which allows us to create non-blocking code with guaranteed writes.
 * These writes will not be re-ordered by instruction reordering. Under the covers it uses the faster store-store barrier, rather than the the slower store-load barrier, which is used when doing a volatile write.
 * One of the trade off with this improved performance is we are limited to a single producer, single consumer.
 * For further information on this see, the blog post <a href="http://robsjava.blogspot.co.uk/2013/06/a-faster-volatile.html">A Faster Volatile</a> by Rob Austin.
 * <p/>
 * <p/>
 * <p/>
 * This queue orders elements FIFO (first-in-first-out).  The
 * <em>head</em> of the queue is that element that has been on the
 * queue the longest time.  The <em>tail</em> of the queue is that
 * element that has been on the queue the shortest time. New elements
 * are inserted at the tail of the queue, and the queue retrieval
 * operations obtain elements at the head of the queue.
 * <p/>
 * <p>This is a classic &quot;bounded buffer&quot;, in which a
 * fixed-capacityd array holds elements inserted by producers and
 * extracted by consumers.  Once created, the capacity cannot be
 * changed.  Attempts to {@link #put put(e)} an element into a full queue
 * will result in the operation blocking; attempts to {@link #take take()} an
 * element from an empty queue will similarly block.
 * <p/>
 * <p>Due to the lock free nature of its  implementation, ordering works on a first come first served basis.<p/>
 * Methods come in four forms, with different ways
 * of handling operations that cannot be satisfied immediately, but may be
 * satisfied at some point in the future:
 * one throws an exception, the second returns a special value (either
 * <tt>null</tt> or <tt>false</tt>, depending on the operation), the third
 * blocks the current thread indefinitely until the operation can succeed,
 * and the fourth blocks for only a given maximum time limit before giving
 * up.  These methods are summarized in the following table:
 * <table BORDER CELLPADDING=3 CELLSPACING=1>
 * <tr>
 * <td></td>
 * <td ALIGN=CENTER><em>Throws exception</em></td>
 * <td ALIGN=CENTER><em>Special value</em></td>
 * <td ALIGN=CENTER><em>Blocks</em></td>
 * <td ALIGN=CENTER><em>Times out</em></td>
 * </tr>
 * <tr>
 * <td><b>Insert</b></td>
 * <td>{@link #add add(e)}</td>
 * <td>{@link #offer offer(e)}</td>
 * <td>{@link #put put(e)}</td>
 * <td>{@link #offer(E, long, java.util.concurrent.TimeUnit) offer(e, time, unit)}</td>
 * </tr>
 * <tr>
 * <td><b>Remove</b></td>
 * <td>{@link #poll poll()}</td>
 * <td>not applicable</td>
 * <td>{@link #take long()}</td>
 * <td>{@link #poll(long, java.util.concurrent.TimeUnit) poll(time, unit)}</td>
 * </tr>
 * <tr>
 * <td><b>Examine</b></td>
 * <td><em>not applicable</em></td>
 * <td><em>not applicable</em></td>
 * <td><em>not applicable</em></td>
 * <td>{@link #peek(long, java.util.concurrent.TimeUnit) peek(time, unit)}</td>>
 * </tr>
 * </table>
 * <p/>
 * <p/>
 * <p>A <tt>uk.co.boundedbuffer.ConcurrentBlockingEQueue</tt> is capacity bounded. At any given
 * time it may have a <tt>remainingCapacity</tt> beyond which no
 * additional elements can be <tt>put</tt> without blocking.
 * <p/>
 * <p> It is not possible to remove an arbitrary element from a queue using
 * <tt>remove(x)</tt>. As this operation would not performed very efficiently.
 * <p/>
 * <p>All of <tt>uk.co.boundedbuffer.ConcurrentBlockingEQueue</tt> methods are thread-safe when used with a single producer and single consumer, internal atomicity
 * is achieved using lock free strategies, such as sping locks.
 * <p/>
 * <p>Like a <tt>BlockingQueue</tt>, the uk.co.boundedbuffer.ConcurrentBlockingEQueue does <em>not</em> intrinsically support
 * any kind of &quot;close&quot; or &quot;shutdown&quot; operation to
 * indicate that no more items will be added.  The needs and usage of
 * such features tend to be implementation-dependent. For example, a
 * common tactic is for producers to insert special
 * <em>end-of-stream</em> or <em>poison</em> objects, that are
 * interpreted accordingly when taken by consumers.
 * <p/>
 * <p/>
 * Usage example, based on a typical producer-consumer scenario.
 * Note that a <tt>BlockingQueue</tt> can ONLY be used with a single
 * producers and single consumer thread.
 * <pre>
 *
 *  Executors.newSingleThreadExecutor().execute(new Runnable() {
 *
 *  public void run() {
 *       queue.put(1);
 *       }
 *  });
 *
 *
 *  Executors.newSingleThreadExecutor().execute(new Runnable() {
 *
 *  public void run() {
 *           final int value = queue.take();
 *       }
 *  });
 * </pre>
 * <p/>
 * <p>Memory consistency effects: As with other concurrent
 * collections, actions in a thread prior to placing an object into a
 * {@code BlockingQueue}
 * <a href="package-summary.html#MemoryVisibility"><i>happen-before</i></a>
 * actions subsequent to the access or removal of that element from
 * the {@code BlockingQueue} in another thread.
 * <p/>
 * <p/>
 * <p/>
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
 * @since 1.1
 */
public class ConcurrentSharedBlockingObjectQueue<E> extends AbstractBlockingQueue implements BlockingQueue<E> {

    // intentionally not volatile, as we are carefully ensuring that the memory barriers are controlled below by other objects
    private final E[] data = (E[]) new Object[capacity];


    /**
     * Creates an BlockingQueue with the default capacity of 1024
     */
    public ConcurrentSharedBlockingObjectQueue() {

    }

    /**
     * @param capacity Creates an BlockingQueue with the given (fixed) capacity
     */
    public ConcurrentSharedBlockingObjectQueue(int capacity) {
        super(capacity);
    }

    public ConcurrentSharedBlockingObjectQueue(int capacity, boolean b) {
        super(capacity);
    }


    public ConcurrentSharedBlockingObjectQueue(int capacity, boolean b, Collection<Integer> elements) {
        super(capacity);
    }

    /**
     * {@inheritDoc}
     */
    public boolean add(E value) {

        // volatile read
        final int writeLocation = this.producerWriteLocation;

        final int nextWriteLocation = getNextWriteLocationThrowIfFull(writeLocation);

        // purposely not volatile
        data[writeLocation] = value;

        setWriteLocation(nextWriteLocation);
        return true;
    }

    /**
     * Retrieves and removes the head of this queue, waiting if necessary
     * until an element becomes available.
     * <p/>
     * the reads must always occur on the same thread
     *
     * @return the head of this queue
     * @throws InterruptedException if interrupted while waiting
     */
    public E take() throws InterruptedException {

        // non volatile read  ( which is quicker )
        final int readLocation = this.consumerReadLocation;

        // sets the nextReadLocation my moving it on by 1, this may cause it it wrap back to the start.
        final int nextReadLocation = blockForReadSpace(readLocation);

        // purposely not volatile as the read memory barrier occurred above when we read 'writeLocation'
        final E value = data[readLocation];

        setReadLocation(nextReadLocation);

        return value;

    }

    /**
     * Retrieves, but does not remove, the head of this queue.
     *
     * @param timeout how long to wait before giving up, in units of
     *                <tt>unit</tt>
     * @param unit    a <tt>TimeUnit</tt> determining how to interpret the
     *                <tt>timeout</tt> parameter
     * @return the head of this queue
     * @throws java.util.concurrent.TimeoutException if timeout time is exceeded
     */

    public E peek(long timeout, TimeUnit unit)
            throws InterruptedException, TimeoutException {

        // non volatile read  ( which is quicker )
        final int readLocation = this.consumerReadLocation;

        blockForReadSpace(timeout, unit, readLocation);

        // purposely not volatile as the read memory barrier occurred above when we read ' this.readLocation'
        return data[readLocation];

    }

    /**
     * {@inheritDoc}
     */
    public boolean offer(E value) {

        // non volatile read  ( which is quicker )
        final int writeLocation = this.producerWriteLocation;

        // sets the nextWriteLocation my moving it on by 1, this may cause it it wrap back to the start.
        final int nextWriteLocation = (writeLocation + 1 == capacity) ? 0 : writeLocation + 1;

        if (nextWriteLocation == capacity) {

            if (readLocation == 0)
                return false;

        } else if (nextWriteLocation == readLocation)
            return false;

        // purposely not volatile see the comment below
        data[writeLocation] = value;

        setWriteLocation(nextWriteLocation);
        return true;
    }


    /**
     * Inserts the specified element into this queue, waiting if necessary
     * for space to become available.
     *
     * @param value the element to add
     * @throws InterruptedException     if interrupted while waiting
     * @throws IllegalArgumentException if some property of the specified
     *                                  element prevents it from being added to this queue
     */
    public void put(E value) throws InterruptedException {

        final int writeLocation1 = this.producerWriteLocation;
        final int nextWriteLocation = blockForWriteSpaceInterruptibly(writeLocation1);

        // purposely not volatile see the comment below
        data[writeLocation1] = value;

        setWriteLocation(nextWriteLocation);
    }

    /**
     * Inserts the specified element into this queue, waiting up to the
     * specified wait time if necessary for space to become available.
     *
     * @param value   the element to add
     * @param timeout how long to wait before giving up, in units of
     *                <tt>unit</tt>
     * @param unit    a <tt>TimeUnit</tt> determining how to interpret the
     *                <tt>timeout</tt> parameter
     * @return <tt>true</tt> if successful, or <tt>false</tt> if
     * the specified waiting time elapses before space is available
     * @throws InterruptedException     if interrupted while waiting
     * @throws ClassCastException       if the class of the specified element
     *                                  prevents it from being added to this queue
     * @throws NullPointerException     if the specified element is null
     * @throws IllegalArgumentException if some property of the specified
     *                                  element prevents it from being added to this queue
     */
    public boolean offer(E value, long timeout, TimeUnit unit)
            throws InterruptedException {

        // non volatile read  ( which is quicker )
        final int writeLocation = this.producerWriteLocation;

        // sets the nextWriteLocation my moving it on by 1, this may cause it it wrap back to the start.
        final int nextWriteLocation = (writeLocation + 1 == capacity) ? 0 : writeLocation + 1;


        if (nextWriteLocation == capacity) {

            final long timeoutAt = System.nanoTime() + unit.toNanos(timeout);

            while (readLocation == 0)
            // this condition handles the case where writer has caught up with the read,
            // we will wait for a read, ( which will cause a change on the read location )
            {
                if (!blockAtAdd(timeoutAt))
                    return false;
            }

        } else {

            final long timeoutAt = System.nanoTime() + unit.toNanos(timeout);

            while (nextWriteLocation == readLocation)
            // this condition handles the case general case where the read is at the start of the backing array and we are at the end,
            // blocks as our backing array is full, we will wait for a read, ( which will cause a change on the read location )
            {
                if (!blockAtAdd(timeoutAt))
                    return false;
            }
        }

        // purposely not volatile see the comment below
        data[writeLocation] = value;

        // the line below, is where the write memory barrier occurs,
        // we have just written back the data in the line above ( which is not require to have a memory barrier as we will be doing that in the line below
        setWriteLocation(nextWriteLocation);

        return true;
    }

    /**
     * Retrieves and removes the head of this queue, waiting up to the
     * specified wait time if necessary for an element to become available.
     *
     * @param timeout how long to wait before giving up, in units of
     *                <tt>unit</tt>
     * @param unit    a <tt>TimeUnit</tt> determining how to interpret the
     *                <tt>timeout</tt> parameter
     * @return the head of this queue, or throws a <tt>TimeoutException</tt> if the
     * specified waiting time elapses before an element is available
     * @throws InterruptedException                  if interrupted while waiting
     * @throws java.util.concurrent.TimeoutException if timeout time is exceeded
     */
    public E poll(long timeout, TimeUnit unit)
            throws InterruptedException {

        final int readLocation = this.consumerReadLocation;
        int nextReadLocation = 0;
        try {
            nextReadLocation = blockForReadSpace(timeout, unit, readLocation);
        } catch (TimeoutException e) {
            return null;
        }

        // purposely non volatile as the read memory barrier occurred when we read 'writeLocation'
        final E value = data[readLocation];
        setReadLocation(nextReadLocation);

        return value;

    }

    /**
     * Retrieves and removes the head of this queue, waiting up to the
     * specified wait time if necessary for an element to become available.
     *
     * @return the head of this queue, or <tt>null</tt> if the
     * specified waiting time elapses before an element is available
     */
    public E poll() {

        final int readLocation = this.consumerReadLocation;

        // sets the nextReadLocation my moving it on by 1, this may cause it it wrap back to the start.
        final int nextReadLocation = (readLocation + 1 == capacity) ? 0 : readLocation + 1;

        if (writeLocation == readLocation)
            return null;

        // purposely not volatile as the read memory barrier occurred when we read 'writeLocation'
        final E value = data[readLocation];
        setReadLocation(nextReadLocation);

        return value;

    }

    @Override
    public boolean remove(Object o) {

        final E[] newData = (E[]) new Object[capacity];

        boolean hasRemovedItem = false;
        int read = this.readLocation;
        int write = this.writeLocation;

        if (read == write)
            return false;

        int i = 0;
        if (read < write) {

            for (int location = read; location <= write; location++) {
                if (o.equals(data[location])) {
                    hasRemovedItem = true;
                } else {
                    newData[i++] = data[location];
                }
            }


        } else {

            for (int location = read; location < capacity; location++) {

                if (!o.equals(data[location])) {
                    hasRemovedItem = true;
                } else {
                    newData[i++] = data[location];
                }

            }

            for (int location = 0; location <= write; location++) {

                if (!o.equals(data[location])) {
                    hasRemovedItem = true;
                } else {
                    newData[i++] = data[location];
                }
            }
        }

        if (!hasRemovedItem)
            return false;

        this.readLocation = 0;
        this.writeLocation = i;
        System.arraycopy(newData, 0, data, 0, i);

        return true;

    }


    @Override
    public boolean containsAll(Collection<?> items) {

        final int read = readLocation;
        final int write = writeLocation;

        if (items.size() == 0)
            return true;

        if (read == write)
            return false;

        for (Object o : items) {
            if (!contains(o))
                return false;
        }


        return true;

    }

    @Override
    public boolean addAll(Collection<? extends E> c) {

        if (this.capacity <= 1)
            throw new NullPointerException("You can not add to a queue of Zero Size.");

        if (this.equals(c))
            throw new IllegalArgumentException();

        // volatile read
        final int writeLocation = this.producerWriteLocation;
        int writeLocation0 = writeLocation;
        int nextWriteLocation = -1;

        for (E e : c) {

            if (e == null)
                throw new NullPointerException("Element to be added, can not be null.");

            nextWriteLocation = getNextWriteLocationThrowIfFull(writeLocation0);

            // purposely not volatile
            data[writeLocation0] = e;
            writeLocation0 = nextWriteLocation;
        }

        if (nextWriteLocation != -1)
            setWriteLocation(nextWriteLocation);
        else
            return false;


        return true;

    }

    @Override
    public boolean removeAll(Collection<?> c) {
        final E[] newData = (E[]) new Object[capacity];

        boolean hasRemovedItem = false;
        int read = this.readLocation;
        int write = this.writeLocation;

        if (read == write)
            return false;

        int i = 0;
        if (read < write) {

            for (int location = read; location < write; location++) {
                if (c.contains(data[location])) {
                    hasRemovedItem = true;
                } else {
                    newData[i++] = data[location];
                }
            }

        } else {

            for (int location = read; location < capacity; location++) {

                if (!c.contains(data[location])) {
                    hasRemovedItem = true;
                } else {
                    newData[i++] = data[location];
                }

            }

            for (int location = 0; location <= write; location++) {

                if (!c.contains(data[location])) {
                    hasRemovedItem = true;
                } else {
                    newData[i++] = data[location];
                }
            }
        }

        if (!hasRemovedItem)
            return false;

        this.readLocation = 0;
        this.writeLocation = i;
        System.arraycopy(newData, 0, data, 0, i);

        return true;

    }

    @Override
    public boolean retainAll(Collection<?> c) {

        final E[] newData = (E[]) new Object[capacity];

        boolean changed = false;
        int read = this.readLocation;
        int write = this.writeLocation;

        if (read == write)
            return false;

        int i = 0;
        if (read < write) {

            for (int location = read; location < write; location++) {
                if (c.contains(data[location])) {
                    newData[i++] = data[location];
                } else {
                    changed = true;
                }
            }

        } else {

            for (int location = read; location < capacity; location++) {

                if (c.contains(data[location])) {
                    newData[i++] = data[location];
                } else {
                    changed = true;
                }


            }

            for (int location = 0; location <= write; location++) {

                if (c.contains(data[location])) {
                    newData[i++] = data[location];
                } else {
                    changed = true;
                }
            }
        }

        if (changed) {

            this.readLocation = 0;
            this.writeLocation = i;
            System.arraycopy(newData, 0, data, 0, i);

            return true;
        }

        return false;

    }


    @Override
    public Iterator<E> iterator() {

        final E[] objects = (E[]) toArray();

        return new Iterator<E>() {

            int i = 0;

            @Override
            public boolean hasNext() {
                return i < objects.length;
            }

            @Override
            public E next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                return objects[i++];
            }

            @Override
            public void remove() {
                i++;
            }
        };

    }

    @Override
    public Object[] toArray() {

        final int read = readLocation;
        final int write = writeLocation;

        if (read == write)
            return new Object[]{};

        int i = 0;

        if (read < write) {
            final Object[] objects = new Object[write - read];
            for (int location = read; location < write; location++) {
                objects[i++] = data[location];
            }

            return objects;
        }


        final Object[] objects = new Object[(capacity - read) + write];

        for (int location = read; location < capacity; location++) {
            objects[i++] = data[location];
        }

        for (int location = 0; location < write; location++) {
            objects[i++] = data[location];
        }

        return objects;
    }

    @Override
    public <T> T[] toArray(T[] result) {

        //  new ArrayBlockingQueue<Integer>(data)
        final int read = readLocation;
        int write = writeLocation;

        if (result.length == 0)
            return result;

        if (read > write)
            write += capacity;

        int size = write - read;

        if (size > result.length)
            result = (T[]) java.lang.reflect.Array.newInstance(
                    result.getClass().getComponentType(), size + 1);

        int i = 0;

        for (int location = read; location < write; location++) {
            result[i++] = (T) data[location];
        }

        if (i < result.length - 1) {
            Arrays.fill((Object[]) result, i, result.length, null);
        }

        // fill with null
        return result;


    }

    @Override

    public int drainTo(Collection<? super E> c) {
        throw new UnsupportedOperationException("not currently supported");
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        throw new UnsupportedOperationException("not currently supported");
    }


    @Override
    public E remove() {

        // non volatile read  ( which is quicker )
        final int readLocation = this.consumerReadLocation;

        // sets the nextReadLocation my moving it on by 1, this may cause it it wrap back to the start.
        final int nextReadLocation = blockForReadSpaceThrowNoSuchElementException(readLocation);

        // purposely not volatile as the read memory barrier occurred above when we read 'writeLocation'
        final E value = data[readLocation];

        setReadLocation(nextReadLocation);

        return value;
    }


    @Override
    public E element() {


        final int readLocation = this.consumerReadLocation;

        // sets the nextReadLocation my moving it on by 1, this may cause it it wrap back to the start.
        final int nextReadLocation = (readLocation + 1 == capacity) ? 0 : readLocation + 1;

        if (writeLocation == readLocation)
            throw new NoSuchElementException();

        // purposely not volatile as the read memory barrier occurred when we read 'writeLocation'
        final E value = data[readLocation];
        setReadLocation(nextReadLocation);

        return value;

    }

    @Override
    public E peek() {
        if (size() == 0)
            return null;
        return data[this.consumerReadLocation];
    }

    /**
     * Returns <tt>true</tt> if this queue contains the specified element.
     * More formally, returns <tt>true</tt> if and only if this queue contains
     * at least one element <tt>e</tt> such that <tt>o.equals(e)</tt>. The behavior of
     * this operation is undefined if modified while the operation is in progress.
     *
     * @param o object to be checked for containment in this queue
     * @return <tt>true</tt> if this queue contains the specified element
     * @throws ClassCastException   if the class of the specified element
     *                              is incompatible with this queue
     *                              (<a href="../Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException if the specified element is null
     *                              (<a href="../Collection.html#optional-restrictions">optional</a>)
     */
    public boolean contains(Object o) {

        if (o == null)
            throw new NullPointerException("object can not be null");

        int read = this.readLocation;
        int write = this.writeLocation;


        if (read == write)
            return false;

        if (read < write) {

            for (int location = read; location < write; location++) {
                if (o.equals(data[location]))
                    return true;
            }

            return false;
        }

        for (int location = read; location < capacity; location++) {

            if (o.equals(data[location]))
                return true;
        }

        for (int location = 0; location < write; location++) {

            if (o.equals(data[location]))
                return true;
        }


        return false;
    }

    /**
     * Removes all available elements from this queue and adds them
     * to the given array. If the target array is smaller than the number of elements then the number of elements read will equal the capacity of the array.
     * This operation may be more
     * efficient than repeatedly polling this queue.  A failure
     * encountered while attempting to add elements to
     * collection <tt>c</tt> may result in elements being in neither,
     * either or both collections when the associated exception is
     * thrown.  Further, the behavior of
     * this operation is undefined if the following methods are called during progress of this operation
     * {@link #take()}. {@link #offer(E)}, {@link #put(E)},  {@link #drainTo(E[], int)}}
     *
     * @param target the collection to transfer elements into
     * @return the number of elements transferred
     * @throws UnsupportedOperationException if addition of elements
     *                                       is not supported by the specified collection
     * @throws ClassCastException            if the class of an element of this queue
     *                                       prevents it from being added to the specified collection
     * @throws NullPointerException          if the specified collection is null
     * @throws IllegalArgumentException      if the specified collection is this
     *                                       queue, or some property of an element of this queue prevents
     *                                       it from being added to the specified collection
     */
    int drainTo(E[] target) {
        return drainTo(target, target.length);
    }

    /**
     * Removes at most the given number of available elements from
     * this queue and adds them to the given collection.  A failure
     * encountered while attempting to add elements to
     * collection <tt>c</tt> may result in elements being in neither,
     * either or both collections when the associated exception is
     * thrown.  Attempts to drain a queue to itself result in
     * <tt>IllegalArgumentException</tt>. Further, the behavior of
     * this operation is undefined if the specified collection is
     * modified while the operation is in progress.
     *
     * @param target      the array to transfer elements into
     * @param maxElements the maximum number of elements to transfer
     * @return the number of elements transferred
     * @throws UnsupportedOperationException if addition of elements
     *                                       is not supported by the specified collection
     * @throws ClassCastException            if the class of an element of this queue
     *                                       prevents it from being added to the specified collection
     * @throws NullPointerException          if the specified collection is null
     * @throws IllegalArgumentException      if the specified collection is this
     *                                       queue, or some property of an element of this queue prevents
     *                                       it from being added to the specified collection
     */
    int drainTo(E[] target, int maxElements) {

        // non volatile read  ( which is quicker )
        int readLocation = this.consumerReadLocation;

        int i = 0;

        // to reduce the number of volatile reads we are going to perform a kind of double check reading on the volatile write location
        int writeLocation = this.writeLocation;

        do {

            // in the for loop below, we are blocked reading unit another item is written, this is because we are empty ( aka capacity()=0)
            // inside the for loop, getting the 'writeLocation', this will serve as our read memory barrier.
            if (writeLocation == readLocation) {

                writeLocation = this.writeLocation;


                if (writeLocation == readLocation) {

                    setReadLocation(readLocation);
                    return i;
                }
            }


            // sets the nextReadLocation my moving it on by 1, this may cause it it wrap back to the start.
            readLocation = (readLocation + 1 == capacity) ? 0 : readLocation + 1;

            // purposely not volatile as the read memory barrier occurred above when we read 'writeLocation'
            target[i] = data[readLocation];


        } while (i <= maxElements);

        setReadLocation(readLocation);

        return maxElements;
    }

    @Override
    public String toString() {


        //  new ArrayBlockingQueue<Integer>(data)
        final int read = readLocation;
        int write = writeLocation;

        if (read == write) {
            return "[]";
        }

        if (read > write)
            write += capacity;

        int size = write - read;

        StringBuilder builder = new StringBuilder("[");

        int i = 0;

        for (int location = read; location < write; location++) {
            builder.append(data[location]).append(',');
        }


        builder.deleteCharAt(builder.length() - 1);
        builder.append("]");

        return builder.toString();


    }
}


