package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.util.DecoratedBufferUnderflowException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.PackageLocal;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.pool.StringBuilderPool;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.queue.impl.WireStorePool;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.EOFException;
import java.io.File;
import java.io.StreamCorruptedException;
import java.text.ParseException;

import static net.openhft.chronicle.bytes.NoBytesStore.NO_PAGE;
import static net.openhft.chronicle.core.UnsafeMemory.MEMORY;
import static net.openhft.chronicle.queue.TailerDirection.*;
import static net.openhft.chronicle.queue.TailerState.*;
import static net.openhft.chronicle.queue.impl.single.ScanResult.*;
import static net.openhft.chronicle.wire.NoDocumentContext.INSTANCE;
import static net.openhft.chronicle.wire.Wires.END_OF_DATA;
import static net.openhft.chronicle.wire.Wires.isEndOfFile;

/**
 * Tailer
 */
class StoreTailer extends AbstractCloseable
        implements ExcerptTailer, SourceContext, ExcerptContext {
    static final int INDEXING_LINEAR_SCAN_THRESHOLD = 70;
    static final StringBuilderPool SBP = new StringBuilderPool();
    static final EOFException EOF_EXCEPTION = new EOFException();
    @NotNull
    private final SingleChronicleQueue queue;
    private final WireStorePool storePool;
    private final LongValue indexValue;
    private final StoreTailerContext context = new StoreTailerContext();
    private final MoveToState moveToState = new MoveToState();
    private final Finalizer finalizer;
    long index; // index of the next read.
    long lastReadIndex; // index of the last read message
    @Nullable
    SingleChronicleQueueStore store;
    private int cycle;
    private TailerDirection direction = TailerDirection.FORWARD;
    private Wire wireForIndex;
    private boolean readAfterReplicaAcknowledged;
    @NotNull
    private TailerState state = UNINITIALISED;
    private long indexAtCreation = Long.MIN_VALUE;
    private boolean readingDocumentFound = false;
    private long address = NO_PAGE;
    private boolean striding = false;

    public StoreTailer(@NotNull final SingleChronicleQueue queue, WireStorePool storePool) {
        this(queue, storePool, null);
    }

    public StoreTailer(@NotNull final SingleChronicleQueue queue, WireStorePool storePool, final LongValue indexValue) {
        boolean error = true;
        try {
            this.queue = queue;
            this.storePool = storePool;
            this.indexValue = indexValue;
            this.setCycle(Integer.MIN_VALUE);
            this.index = 0;
            queue.addCloseListener(this);

            if (indexValue == null) {
                toStart();
            } else {
                moveToIndex(indexValue.getVolatileValue());
            }
            finalizer = Jvm.isResourceTracing() ? new Finalizer() : null;
            error = false;

        } finally {
            if (error)
                close();
        }
    }

    @Override
    public @NotNull StoreTailer disableThreadSafetyCheck(boolean disableThreadSafetyCheck) {
        final Wire privateWire = privateWire();
        if (privateWire != null) {
            ((MappedBytes) privateWire.bytes()).disableThreadSafetyCheck(disableThreadSafetyCheck);
        }
        super.disableThreadSafetyCheck(disableThreadSafetyCheck);
        return this;
    }

    @Override
    public void clearUsedByThread() {
        super.clearUsedByThread();
        final Wire privateWire = privateWire();
        if (privateWire != null) {
            ((MappedBytes) privateWire.bytes()).clearUsedByThread();
        }
    }

    @Override
    public boolean readDocument(@NotNull final ReadMarshallable reader) {
        throwExceptionIfClosed();

        try (@NotNull DocumentContext dc = readingDocument(false)) {
            if (!dc.isPresent())
                return false;
            reader.readMarshallable(dc.wire());
        }
        return true;
    }

    @Override
    @NotNull
    public DocumentContext readingDocument() {
//        throwExceptionIfClosed();

        // trying to create an initial document without a direction should not consume a message
        final long index = index();
        if (direction == NONE && (index == indexAtCreation || index == 0) && !readingDocumentFound) {
            return INSTANCE;
        }
        return readingDocument(false);
    }

    @Override
    protected void performClose() {
        Closeable.closeQuietly(indexValue);
        // the wire ref count will be released here by setting it to null
        context.wire(null);
        final Wire w0 = wireForIndex;
        if (w0 != null)
            w0.bytes().release(INIT);
        wireForIndex = null;
        releaseStore();
    }

    @Override
    public Wire wire() {
        throwExceptionIfClosed();

        return privateWire();
    }

    public Wire privateWire() {
        return context.wire();
    }

    @Override
    public Wire wireForIndex() {
        throwExceptionIfClosed();

        return wireForIndex;
    }

    @Override
    public long timeoutMS() {
        return queue.timeoutMS;
    }

    @Override
    public int sourceId() {
        return queue.sourceId;
    }

    @NotNull
    @Override
    public String toString() {
        final long index = index();
        return "StoreTailer{" +
                "index sequence=" + queue.rollCycle().toSequenceNumber(index) +
                ", index cycle=" + queue.rollCycle().toCycle(index) +
                ", store=" + store + ", queue=" + queue + '}';
    }

    @NotNull
    @Override
    public DocumentContext readingDocument(final boolean includeMetaData) {
        DocumentContext documentContext = readingDocument0(includeMetaData);
        // this check was added after a strange behaviour seen by one client. I should be impossible.
        if (documentContext.wire() != null)
            if (documentContext.wire().bytes().readRemaining() >= 1 << 30)
                throw new AssertionError("readRemaining " + documentContext.wire().bytes().readRemaining());
        return documentContext;
    }

    DocumentContext readingDocument0(final boolean includeMetaData) {
        throwExceptionIfClosed();

        try {
            boolean next = false;
            boolean tryAgain = true;
            if (state == FOUND_IN_CYCLE) {
                try {
                    next = inACycle(includeMetaData);

                    tryAgain = false;
                } catch (EOFException eof) {
                    state = TailerState.END_OF_CYCLE;
                }
            }

            if (tryAgain)
                next = next0(includeMetaData);

            if (context.present(next)) {
                Bytes<?> bytes = context.wire().bytes();
                context.setStart(bytes.readPosition() - 4);
                readingDocumentFound = true;
                address = bytes.addressForRead(bytes.readPosition(), 4);
                this.lastReadIndex = this.index();
                return context;
            }

            RollCycle rollCycle = queue.rollCycle();
            if (state == CYCLE_NOT_FOUND && direction == FORWARD) {
                int firstCycle = queue.firstCycle();
                if (rollCycle.toCycle(index()) < firstCycle)
                    toStart();
            } else if (!next && state == CYCLE_NOT_FOUND && cycle != queue.cycle()) {
                // appenders have moved on, it's possible that linearScan is hitting EOF, which is ignored
                // since we can't find an entry at current index, indicate that we're at the end of a cycle
                state = TailerState.END_OF_CYCLE;
            }

            setAddress(context.wire() != null);

        } catch (StreamCorruptedException e) {
            throw new IllegalStateException(e);
        } catch (UnrecoverableTimeoutException notComplete) {
            // so treat as empty.
        } catch (DecoratedBufferUnderflowException e) {
            // read-only tailer view is fixed, a writer could continue past the end of the view
            // at the point this tailer was created. Log a warning and return no document.
            if (queue.isReadOnly()) {
                Jvm.warn().on(StoreTailer.class,
                        "Tried to read past the end of a read-only view. " +
                                "Underlying data store may have grown since this tailer was created.", e);
            } else {
                throw e;
            }
        }
        return INSTANCE;
    }

    // throws UnrecoverableTimeoutException
    private boolean next0(final boolean includeMetaData) throws StreamCorruptedException {
        for (int i = 0; i < 1000; i++) {
            switch (state) {
                case UNINITIALISED:
                    final long firstIndex = queue.firstIndex();
                    if (firstIndex == Long.MAX_VALUE)
                        return false;
                    if (includeMetaData) {
                        if (moveToCycle(queue.rollCycle().toCycle(firstIndex))) {
                            inACycleFound(wire().bytes());
                            return true;
                        }
                    } else {
                        if (!moveToIndexInternal(firstIndex))
                            return false;
                    }
                    break;

                case NOT_REACHED_IN_CYCLE:
                    if (!moveToIndexInternal(index))
                        return false;
                    break;

                case FOUND_IN_CYCLE: {
                    try {
                        return inACycle(includeMetaData);
                    } catch (EOFException eof) {
                        state = TailerState.END_OF_CYCLE;
                    }
                    break;
                }

                case END_OF_CYCLE:
                    if (endOfCycle()) {
                        continue;
                    }

                    return false;

                case BEYOND_START_OF_CYCLE:
                    if (beyondStartOfCycle())
                        continue;
                    return false;

                case CYCLE_NOT_FOUND:
                    if (nextCycleNotFound())
                        continue;
                    return false;

                default:
                    throw new AssertionError("state=" + state);
            }
        }

        throw new IllegalStateException("Unable to progress to the next cycle, state=" + state);
    }

    private boolean endOfCycle() {
        final long oldIndex = this.index();
        final int currentCycle = queue.rollCycle().toCycle(oldIndex);
        final long nextIndex = nextIndexWithNextAvailableCycle(currentCycle);

        if (nextIndex != Long.MIN_VALUE) {
            return nextEndOfCycle(queue.rollCycle().toCycle(nextIndex));
        } else {
            state = END_OF_CYCLE;
        }
        return false;
    }

    private boolean beyondStartOfCycle() throws StreamCorruptedException {
        if (direction == FORWARD) {
            state = UNINITIALISED;
            return true;
        } else if (direction == BACKWARD) {
            return beyondStartOfCycleBackward();
        }
        throw new AssertionError("direction not set, direction=" + direction);
    }

    private boolean nextEndOfCycle(final int nextCycle) {
        if (moveToCycle(nextCycle)) {
            state = FOUND_IN_CYCLE;
            return true;
        }
        if (state == END_OF_CYCLE) {
            return true;
        }
        if (cycle < queue.lastCycle()) {
            // we have encountered an empty file without an EOF marker
            state = END_OF_CYCLE;
            return true;
        }
        // We are here because we are waiting for an entry to be written to this file.
        // Winding back to the previous cycle results in a re-initialisation of all the objects => garbage
        cycle(nextCycle);
        state = CYCLE_NOT_FOUND;
        return false;
    }

    @Override
    public long lastReadIndex() {
        return this.lastReadIndex;
    }

    private boolean beyondStartOfCycleBackward() throws StreamCorruptedException {
        // give the position of the last entry and
        // flag we want to count it even though we don't know if it will be meta data or not.

        final boolean foundCycle = cycle(queue.rollCycle().toCycle(index()));

        if (foundCycle) {
            final long lastSequenceNumberInThisCycle = store().sequenceForPosition(this, Long.MAX_VALUE, false);
            final long nextIndex = queue.rollCycle().toIndex(this.cycle, lastSequenceNumberInThisCycle);
            moveToIndexInternal(nextIndex);
            state = FOUND_IN_CYCLE;
            return true;
        }

        final int cycle = queue.rollCycle().toCycle(index());
        final long nextIndex = nextIndexWithNextAvailableCycle(cycle);

        if (nextIndex != Long.MIN_VALUE) {
            moveToIndexInternal(nextIndex);
            state = FOUND_IN_CYCLE;
            return true;
        }

        state = BEYOND_START_OF_CYCLE;
        return false;
    }

    private boolean nextCycleNotFound() {
        if (index() == Long.MIN_VALUE) {
            if (this.store != null)
                queue.closeStore(this.store);
            this.store = null;
            return false;
        }

        if (moveToIndexInternal(index())) {
            state = FOUND_IN_CYCLE;
            return true;
        }
        return false;
    }

    private boolean inACycle(final boolean includeMetaData) throws EOFException {
        if (readAfterReplicaAcknowledged && inACycleCheckRep()) return false;

        if (direction != TailerDirection.FORWARD && !inACycleNotForward()) return false;

        final Wire wire = privateWire();
        if (wire == null) {
            throwExceptionIfClosed();
            return false;
        }

        final Bytes<?> bytes = wire.bytes();
        return inACycle2(includeMetaData, wire, bytes);
    }

    private boolean inACycle2(boolean includeMetaData, Wire wire, Bytes<?> bytes) throws EOFException {
        bytes.readLimitToCapacity();

        switch (wire.readDataHeader(includeMetaData)) {
            case NONE:
                // no more polling - appender will always write (or recover) EOF
                return false;
            case META_DATA:
                context.metaData(true);
                break;
            case DATA:
                context.metaData(false);
                break;
            case EOF:
                throw EOF_EXCEPTION;
        }

        inACycleFound(bytes);
        return true;
    }

    private boolean inACycleCheckRep() {
        final long lastSequenceAck = queue.lastAcknowledgedIndexReplicated();
        final long index = index();
        return index > lastSequenceAck;
    }

    private boolean inACycleNotForward() {
        if (!moveToIndexInternal(index())) {
            try {
                // after toEnd() call, index is past the end of the queue
                // so try to go back one (to the last record in the queue)
                if ((int) queue.rollCycle().toSequenceNumber(index()) < 0) {
                    long lastSeqNum = store.lastSequenceNumber(this);
                    if (lastSeqNum == -1) {
                        windBackCycle(cycle);
                        return moveToIndexInternal(index());
                    }

                    return moveToIndexInternal(queue.rollCycle().toIndex(cycle, lastSeqNum));
                }
                if (!moveToIndexInternal(index() - 1)) {
                    return false;
                }
            } catch (Exception e) {
                // can happen if index goes negative
                return false;
            }
        }
        return true;
    }

    private void inACycleFound(@NotNull final Bytes<?> bytes) {
        context.closeReadLimit(bytes.capacity());
        privateWire().readAndSetLength(bytes.readPosition());
        final long end = bytes.readLimit();
        context.closeReadPosition(end);
    }

    private long nextIndexWithNextAvailableCycle(final int cycle) {
        try {
            return nextIndexWithNextAvailableCycle0(cycle);
        } catch (MissingStoreFileException e) {
            queue.refreshDirectoryListing();
            return nextIndexWithNextAvailableCycle0(cycle);
        }
    }

    private long nextIndexWithNextAvailableCycle0(final int cycle) {
        assert cycle != Integer.MIN_VALUE : "cycle == Integer.MIN_VALUE";

        if (cycle > queue.lastCycle() || direction == TailerDirection.NONE) {
            return Long.MIN_VALUE;
        }

        long nextIndex;
        final int nextCycle = cycle + direction.add();
        final boolean found = cycle(nextCycle);
        if (found)
            nextIndex = nextIndexWithinFoundCycle(nextCycle);
        else
            try {
                final int nextCycle0 = queue.nextCycle(this.cycle, direction);
                if (nextCycle0 == -1)
                    return Long.MIN_VALUE;

                nextIndex = nextIndexWithinFoundCycle(nextCycle0);

            } catch (ParseException e) {
                throw new IllegalStateException(e);
            }

        if (Jvm.isResourceTracing()) {
            final int nextIndexCycle = queue.rollCycle().toCycle(nextIndex);
            if (nextIndex != Long.MIN_VALUE && nextIndexCycle - 1 != cycle) {

                /*
                 * lets say that you were using a roll cycle of TEST_SECONDLY
                 * and you wrote a message to the queue, if you created a tailer and read the first message,
                 * then waited around 22 seconds before writing the next message, when the tailer
                 * came to read the next message, there would be a gap of 22 cycle files
                 * that did not exist, that is what this is reporting. If you are using daily rolling,
                 * and writing every day, you should not see this message.
                 */

                Jvm.debug().on(getClass(), "Rolled " + (nextIndexCycle - cycle) + " " + "times to find the " +
                        "next cycle file. This can occur if your appenders have not written " +
                        "anything for a while, leaving the cycle files with a gap.");
            }
        }

        return nextIndex;
    }

    private long nextIndexWithinFoundCycle(final int nextCycle) {
        state = FOUND_IN_CYCLE;
        if (direction == FORWARD)
            return queue.rollCycle().toIndex(nextCycle, 0);

        if (direction == BACKWARD) {
            try {
                long lastSequenceNumber0 = store().lastSequenceNumber(this);
                return queue.rollCycle().toIndex(nextCycle, lastSequenceNumber0);

            } catch (Exception e) {
                throw new AssertionError(e);
            }
        } else {
            throw new IllegalStateException("direction=" + direction);
        }
    }

    /**
     * @return provides an index that includes the cycle number
     */
    @Override
    public long index() {
        return indexValue == null ? this.index : indexValue.getValue();
    }

    @Override
    public int cycle() {
        return this.cycle;
    }

    @Override
    public boolean moveToIndex(final long index) {
        throwExceptionIfClosed();

        if (moveToState.canReuseLastIndexMove(index, state, direction, queue, privateWire())) {
            return setAddress(true);
        } else if (moveToState.indexIsCloseToAndAheadOfLastIndexMove(index, state, direction, queue)) {
            final long knownIndex = moveToState.lastMovedToIndex;
            final boolean found =
                    this.store.linearScanTo(index, knownIndex, this,
                            moveToState.readPositionAtLastMove) == ScanResult.FOUND;
            if (found) {
                index(index);
                moveToState.onSuccessfulScan(index, direction, privateWire().bytes().readPosition());
            }
            return setAddress(found);
        }

        return moveToIndexInternal(index);
    }

    @Override
    public boolean moveToCycle(final int cycle) {
        throwExceptionIfClosed();

        moveToState.indexMoveCount++;
        final ScanResult scanResult = moveToCycleResult0(cycle);
        setAddress(scanResult == FOUND);
        return scanResult == FOUND;
    }

    private boolean setAddress(final boolean found) {
        final Wire wire = privateWire();
        if (wire == null) {
            address = NO_PAGE;
            return false;
        }
        final Bytes<?> bytes = wire.bytes();
        address = found ? bytes.addressForRead(bytes.readPosition(), 4) : NO_PAGE;
        return found;
    }

    private ScanResult moveToCycleResult0(final int cycle) {
        if (cycle < 0)
            return NOT_REACHED;
        final RollCycle rollCycle = queue.rollCycle();
//        if (Jvm.isResourceTracing()) {
//            Jvm.debug().on(getClass(), "moveToIndex: " + Long.toHexString(cycle) + " " + Long.toHexString(sequenceNumber));
//        }

        // moves to the expected cycle
        if (!cycle(cycle))
            return NOT_REACHED;

        long index = rollCycle.toIndex(cycle, 0);
        index(index);
        this.store().moveToStartForRead(this);
        final Bytes<?> bytes = privateWire().bytes();

        state = FOUND_IN_CYCLE;
        moveToState.onSuccessfulLookup(index, direction, bytes.readPosition());

        return FOUND;
    }

    private ScanResult moveToIndexResult0(final long index) {
        if (index < 0)
            return NOT_REACHED;
        final RollCycle rollCycle = queue.rollCycle();
        final int cycle = rollCycle.toCycle(index);
        final long sequenceNumber = rollCycle.toSequenceNumber(index);
//        if (Jvm.isResourceTracing()) {
//            Jvm.debug().on(getClass(), "moveToIndex: " + Long.toHexString(cycle) + " " + Long.toHexString(sequenceNumber));
//        }

        // moves to the expected cycle
        if (!cycle(cycle))
            return NOT_REACHED;

        index(index);
        final ScanResult scanResult = this.store().moveToIndexForRead(this, sequenceNumber);
        final Bytes<?> bytes = privateWire().bytes();
        switch (scanResult) {
            case FOUND:
                state = FOUND_IN_CYCLE;
                moveToState.onSuccessfulLookup(index, direction, bytes.readPosition());
                break;

            case NOT_REACHED:
                state = NOT_REACHED_IN_CYCLE;
                break;
            case NOT_FOUND:
                if (this.cycle < this.queue.lastCycle()) {
                    state = END_OF_CYCLE;
                    return END_OF_FILE;
                }
                break;
            case END_OF_FILE:
                state = END_OF_CYCLE;
                break;
        }

        return scanResult;
    }

    ScanResult moveToIndexResult(final long index) {
        final ScanResult scanResult = moveToIndexResult0(index);
        setAddress(scanResult == FOUND);
        return scanResult;
    }

    private ExcerptTailer doToStart() {
        assert direction != BACKWARD;
        final int firstCycle = queue.firstCycle();
        if (firstCycle == Integer.MAX_VALUE) {
            state = UNINITIALISED;
            address = NO_PAGE;
            return this;
        }
        if (firstCycle != this.cycle) {
            // moves to the expected cycle
            final boolean found = cycle(firstCycle);
            if (found)
                state = FOUND_IN_CYCLE;
            else if (store != null)
                throw new MissingStoreFileException("Missing first store file cycle=" + firstCycle);
        }
        index(queue.rollCycle().toIndex(cycle, 0));

        state = FOUND_IN_CYCLE;
        Wire wire = privateWire();
        if (wire != null) {
            wire.bytes().readPosition(0);
            address = wire.bytes().addressForRead(0);
        }
        return this;
    }

    @NotNull
    @Override
    public final ExcerptTailer toStart() {
        try {
            return doToStart();
        } catch (MissingStoreFileException e) {
            queue.refreshDirectoryListing();
            return doToStart();
        }
    }

    private boolean moveToIndexInternal(final long index) {
        moveToState.indexMoveCount++;
        final ScanResult scanResult = moveToIndexResult0(index);
        setAddress(scanResult == FOUND);
        return scanResult == FOUND;
    }

    /**
     * gives approximately the last index, can not be relied on as the last index may have changed just after this was called. For this reason, this
     * code is not in queue as it should only be an internal method
     *
     * @return the last index at the time this method was called, or Long.MIN_VALUE if none.
     */
    private long approximateLastIndex() {

        final int lastCycle = queue.lastCycle();
        try {
            if (lastCycle == Integer.MIN_VALUE)
                return Long.MIN_VALUE;

            return approximateLastCycle2(lastCycle);

        } catch (@NotNull StreamCorruptedException | UnrecoverableTimeoutException e) {
            throw new IllegalStateException(e);
        }
    }

    private long approximateLastCycle2(int lastCycle) throws StreamCorruptedException {
        RollCycle rollCycle = queue.rollCycle();

        final SingleChronicleQueueStore wireStore = (cycle == lastCycle) ? this.store : queue.storeForCycle(
                lastCycle, queue.epoch(), false, this.store);
        this.setCycle(lastCycle);
        if (wireStore == null)
            throw new MissingStoreFileException("Store not found for cycle " + Long.toHexString(lastCycle) + ". Probably the files were removed? queue=" + queue.fileAbsolutePath());

        if (this.store != wireStore) {
            releaseStore();
            this.store = wireStore;
            resetWires();
        }
        // give the position of the last entry and
        // flag we want to count it even though we don't know if it will be meta data or not.

        final long sequenceNumber = this.store.lastSequenceNumber(this);

        // fixes #378
        if (sequenceNumber == -1L) {
            // nothing has been written yet, so point to start of cycle
            long prevCycle = queue.firstCycle();
            while (prevCycle < lastCycle) {
                lastCycle--;
                try {
                    return approximateLastCycle2(lastCycle);
                } catch (MissingStoreFileException e) {
                    // try again.
                }
            }
            return rollCycle.toIndex(lastCycle, 0L);
        }
        return rollCycle.toIndex(lastCycle, sequenceNumber);
    }

    private boolean headerNumberCheck(@NotNull final AbstractWire wire) {

        wire.headNumberCheck((actual, position) -> {
            try {
                final long expecting = store.sequenceForPosition(this, position, false);
                if (actual == expecting)
                    return true;
                Jvm.warn().on(getClass(), new AssertionError("header number check failed " +
                        "expecting=" + expecting +
                        "  !=  actual=" + actual));

                return false;
            } catch (Exception e) {
                Jvm.warn().on(getClass(), "", e);
                return false;
            }
        });

        return true;
    }

    private void resetWires() {
        final WireType wireType = queue.wireType();

        final MappedBytes bytes = store.bytes();
        bytes.disableThreadSafetyCheck(disableThreadSafetyCheck());
        final Wire wire2 = wireType.apply(bytes);
        wire2.usePadding(store.dataVersion() > 0);
        final AbstractWire wire = (AbstractWire) readAnywhere(wire2);
        assert !QueueSystemProperties.CHECK_INDEX || headerNumberCheck(wire);
        this.context.wire(wire);
        wire.parent(this);

        final Wire wireForIndexOld = wireForIndex;
        wireForIndex = readAnywhere(wireType.apply(store().bytes()));
        assert !QueueSystemProperties.CHECK_INDEX || headerNumberCheck((AbstractWire) wireForIndex);
        assert wire != wireForIndexOld;

        if (wireForIndexOld != null)
            wireForIndexOld.bytes().releaseLast();
    }

    @NotNull
    private Wire readAnywhere(@NotNull final Wire wire) {
        final Bytes<?> bytes = wire.bytes();
        bytes.readLimitToCapacity();
        wire.usePadding(store.dataVersion() > 0);
        return wire;
    }

    @NotNull
    @Override
    public ExcerptTailer toEnd() {
        throwExceptionIfClosed();

        if (direction.equals(TailerDirection.BACKWARD)) {
            return callOriginalToEnd();
        }

        return callOptimizedToEnd();
    }

    private ExcerptTailer callOptimizedToEnd() {
        try {
            return optimizedToEnd();
        } catch (MissingStoreFileException e) {
            queue.refreshDirectoryListing();
            return optimizedToEnd();
        }
    }

    @NotNull
    private ExcerptTailer callOriginalToEnd() {
        try {
            return originalToEnd();
        } catch (NotReachedException e) {
            queue.refreshDirectoryListing();
            // due to a race condition, where the queue rolls as we are processing toEnd()
            // we may get a NotReachedException  ( see https://github.com/OpenHFT/Chronicle-Queue/issues/702 )
            // hence are are just going to retry.
            try {
                return originalToEnd();
            } catch (Exception ex) {
                Jvm.warn().on(getClass(), "Unable to find toEnd() so winding to the start " + ex);
                return toStart();
            }
        }
    }

    @Override
    public ExcerptTailer striding(final boolean striding) {
        throwExceptionIfClosedInSetter();

        this.striding = striding;
        return this;
    }

    @Override
    public boolean striding() {
        return striding;
    }

    @NotNull
    private ExcerptTailer optimizedToEnd() {
        final RollCycle rollCycle = queue.rollCycle();
        final int lastCycle = queue.lastCycle();
        try {
            if (lastCycle == Integer.MIN_VALUE) {
                if (state() == TailerState.CYCLE_NOT_FOUND)
                    state = UNINITIALISED;
                setAddress(state == FOUND_IN_CYCLE);
                return this;
            }

            // TODO fix this so it doesn't replace the same store.
            final SingleChronicleQueueStore wireStore = queue.storeForCycle(
                    lastCycle, queue.epoch(), false, this.store);
            this.setCycle(lastCycle);
            if (wireStore == null)
                throw new MissingStoreFileException("Store not found for cycle " + Long.toHexString(lastCycle) + ". Probably the files were removed? queue=" + queue.fileAbsolutePath());

            if (this.store != wireStore) {
                releaseStore();
                this.store = wireStore;
                resetWires();
            }
            // give the position of the last entry and
            // flag we want to count it even though we don't know if it will be meta data or not.

            final long sequenceNumber = store.moveToEndForRead(privateWire());

            // fixes #378
            if (sequenceNumber == -1L) {
                return callOriginalToEnd();
            }

            final Bytes<?> bytes = privateWire().bytes();
            state = isEndOfFile(bytes.readVolatileInt(bytes.readPosition())) ? END_OF_CYCLE : FOUND_IN_CYCLE;

            index(rollCycle.toIndex(lastCycle, sequenceNumber));

            setAddress(state == FOUND_IN_CYCLE);
        } catch (@NotNull UnrecoverableTimeoutException e) {
            throw new IllegalStateException(e);
        }

        return this;
    }

    @NotNull
    public ExcerptTailer originalToEnd() {
        throwExceptionIfClosed();

        long index = approximateLastIndex();

        if (index == Long.MIN_VALUE) {
            if (state() == TailerState.CYCLE_NOT_FOUND)
                state = UNINITIALISED;
            return this;
        }
        ScanResult scanResult = moveToIndexResult(index);
        switch (scanResult) {
            case NOT_FOUND:
                if (moveToIndexResult(index - 1) == FOUND)
                    state = FOUND_IN_CYCLE;
                break;

            case FOUND:
                if (direction == FORWARD) {
                    final ScanResult result = moveToIndexResult(++index);
                    switch (result) {
                        case NOT_REACHED:
                            throw new NotReachedException("NOT_REACHED after FOUND");
                        case FOUND:
                            // the end moved!!
                        case NOT_FOUND:
                            state = FOUND_IN_CYCLE;
                            break;
                        case END_OF_FILE:
                            state = END_OF_CYCLE;
                            break;
                        default:
                            throw new IllegalStateException("Unknown ScanResult: " + result);
                    }
                }
                break;
            case NOT_REACHED:
                throw new NotReachedException("NOT_REACHED index: " + Long.toHexString(index));
            case END_OF_FILE:
                state = END_OF_CYCLE;
                break;
            default:
                throw new IllegalStateException("Unknown ScanResult: " + scanResult);
        }

        return this;

    }

    @Override
    public TailerDirection direction() {
        return direction;
    }

    @NotNull
    @Override
    public ExcerptTailer direction(@NotNull final TailerDirection direction) {
        throwExceptionIfClosedInSetter();

        final TailerDirection oldDirection = this.direction();
        this.direction = direction;
        if (oldDirection == TailerDirection.BACKWARD &&
                direction == TailerDirection.FORWARD) {
            moveToIndexInternal(index());
        }

        return this;
    }

    @Override
    @NotNull
    public ChronicleQueue queue() {
        return queue;
    }

    @PackageLocal
    void incrementIndex() {
        final RollCycle rollCycle = queue.rollCycle();
        final long index = this.index();
        if (index == -1 && direction == FORWARD) {
            index0(0);
            return;
        }
        long seq = rollCycle.toSequenceNumber(index);
        final int cycle = rollCycle.toCycle(index);

        seq += direction.add();
        switch (direction) {
            case NONE:
                break;
            case FORWARD:
                // if it runs out of seq number it will flow over to tomorrows cycle file
                if (rollCycle.toSequenceNumber(seq) < seq) {
                    if (this.cycle != cycle + 1) {
                        cycle(cycle + 1);
                        Jvm.warn().on(getClass(),
                                "we have run out of sequence numbers, so will start to write to " +
                                        "the next .cq4 file, the new cycle=" + cycle);
                    }
                    seq = 0;
                }
                break;
            case BACKWARD:
                if (seq < 0) {
                    windBackCycle(cycle);
                    return;
                } else if (seq > 0 && striding) {
                    seq -= seq % rollCycle.defaultIndexSpacing();
                }
                break;
        }
        index0(rollCycle.toIndex(cycle, seq));

    }

    private void windBackCycle(int cycle) {
        final long first = queue.firstCycle();
        while (--cycle >= first)
            if (tryWindBack(cycle))
                return;

        this.index(queue.rollCycle().toIndex(cycle, -1));
        this.state = BEYOND_START_OF_CYCLE;
    }

    private boolean tryWindBack(final int cycle) {
        final long count = queue.exactExcerptsInCycle(cycle);
        if (count <= 0)
            return false;
        final RollCycle rollCycle = queue.rollCycle();
        moveToIndexInternal(rollCycle.toIndex(cycle, count - 1));
        this.state = FOUND_IN_CYCLE;
        return true;
    }

    void index0(final long index) {
        if (indexValue == null)
            this.index = index;
        else
            indexValue.setValue(index);
    }

    // DON'T INLINE THIS METHOD, as it's used by enterprise chronicle queue
    void index(final long index) {
        index0(index);

        if (indexAtCreation == Long.MIN_VALUE) {
            indexAtCreation = index;
        }

        moveToState.reset();
    }

    private boolean cycle(final int cycle) {
        if (this.cycle == cycle && (state == FOUND_IN_CYCLE || state == NOT_REACHED_IN_CYCLE))
            return true;

        final SingleChronicleQueueStore nextStore = queue.storeForCycle(
                cycle, queue.epoch(), false, this.store);

        if (nextStore == null && store == null)
            return false;

        if (nextStore == null) {
            if (direction == BACKWARD)
                state = BEYOND_START_OF_CYCLE;
            else
                state = CYCLE_NOT_FOUND;
            return false;
        }

        if (nextStore == store) {
            return true;
        }

        releaseStore();

        context.wire(null);
        store = nextStore;
        state = FOUND_IN_CYCLE;
        setCycle(cycle);
        resetWires();
        final Wire wire = privateWire();
        wire.parent(this);
        wire.pauser(queue.pauserSupplier.get());
        return true;
    }

    void releaseStore() {
        if (store != null) {
            storePool.closeStore(store);
            store = null;
        }
        state = UNINITIALISED;
    }

    @Override
    public void readAfterReplicaAcknowledged(final boolean readAfterReplicaAcknowledged) {
        throwExceptionIfClosed();

        this.readAfterReplicaAcknowledged = readAfterReplicaAcknowledged;
    }

    @Override
    public boolean readAfterReplicaAcknowledged() {
        throwExceptionIfClosed();

        return readAfterReplicaAcknowledged;
    }

    @NotNull
    @Override
    public TailerState state() {
        return state;
    }

    @NotNull
    @Override
    public ExcerptTailer afterLastWritten(@NotNull final ChronicleQueue queue) {
        throwExceptionIfClosed();

        if (queue == this.queue)
            throw new IllegalArgumentException("You must pass the queue written to, not the queue read");
        try (@NotNull final ExcerptTailer tailer = queue.createTailer()
                .direction(BACKWARD)
                .toEnd()) {

            @NotNull final VanillaMessageHistory messageHistory = new VanillaMessageHistory();

            while (true) {
                try (DocumentContext context = tailer.readingDocument()) {
                    if (!context.isPresent()) {
                        toStart();
                        return this;
                    }

                    final MessageHistory veh = SCQTools.readHistory(context, messageHistory);
                    if (veh == null)
                        continue;

                    int i = veh.sources() - 1;
                    if (i < 0)
                        continue;
                    if (veh.sourceId(i) != this.sourceId())
                        continue;

                    final long sourceIndex = veh.sourceIndex(i);
                    if (!moveToIndexInternal(sourceIndex)) {
                        final String errorMessage = String.format(
                                "Unable to move to sourceIndex %s in queue %s",
                                Long.toHexString(sourceIndex), this.queue.fileAbsolutePath());
                        throw new IORuntimeException(errorMessage + extraInfo(tailer, messageHistory));
                    }
                    try (DocumentContext content = readingDocument()) {
                        if (!content.isPresent()) {
                            final String errorMessage = String.format(
                                    "No readable document found at sourceIndex %s in queue %s",
                                    Long.toHexString(sourceIndex + 1), this.queue.fileAbsolutePath());
                            throw new IORuntimeException(errorMessage + extraInfo(tailer, messageHistory));
                        }
                        // skip this message and go to the next.
                    }
                    return this;
                }
            }
        }
    }

    private String extraInfo(@NotNull final ExcerptTailer tailer, @NotNull final VanillaMessageHistory messageHistory) {
        return String.format(
                ". That sourceIndex was determined fom the last entry written to queue %s " +
                        "(message index %s, message history %s). If source queue is replicated then " +
                        "sourceIndex may not have been replicated yet",
                tailer.queue().fileAbsolutePath(), Long.toHexString(tailer.index()), WireType.TEXT.asString(messageHistory));
    }

    public void setCycle(final int cycle) {
        this.cycle = cycle;
    }

    // visible for testing
    int getIndexMoveCount() {
        return moveToState.indexMoveCount;
    }

    @Deprecated // Should not be providing accessors to reference-counted objects
    @NotNull
    WireStore store() {
        if (store == null)
            setCycle(cycle());
        return store;
    }

    @Override
    public File currentFile() {
        SingleChronicleQueueStore store = this.store;
        return store == null ? null : store.currentFile();
    }

    static final class MoveToState {
        private long lastMovedToIndex = Long.MIN_VALUE;
        private TailerDirection directionAtLastMoveTo = TailerDirection.NONE;
        private long readPositionAtLastMove = Long.MIN_VALUE;
        private int indexMoveCount = 0;

        void onSuccessfulLookup(final long movedToIndex,
                                final TailerDirection direction,
                                final long readPosition) {
            this.lastMovedToIndex = movedToIndex;
            this.directionAtLastMoveTo = direction;
            this.readPositionAtLastMove = readPosition;
        }

        void onSuccessfulScan(final long movedToIndex,
                              final TailerDirection direction,
                              final long readPosition) {
            this.lastMovedToIndex = movedToIndex;
            this.directionAtLastMoveTo = direction;
            this.readPositionAtLastMove = readPosition;
        }

        void reset() {
            lastMovedToIndex = Long.MIN_VALUE;
            directionAtLastMoveTo = TailerDirection.NONE;
            readPositionAtLastMove = Long.MIN_VALUE;
        }

        private boolean indexIsCloseToAndAheadOfLastIndexMove(final long index,
                                                              final TailerState state,
                                                              final TailerDirection direction,
                                                              final ChronicleQueue queue) {
            return lastMovedToIndex != Long.MIN_VALUE &&
                    index - lastMovedToIndex < INDEXING_LINEAR_SCAN_THRESHOLD &&
                    state == FOUND_IN_CYCLE &&
                    direction == directionAtLastMoveTo &&
                    queue.rollCycle().toCycle(index) == queue.rollCycle().toCycle(lastMovedToIndex) &&
                    index > lastMovedToIndex;
        }

        private boolean canReuseLastIndexMove(final long index,
                                              final TailerState state,
                                              final TailerDirection direction,
                                              final ChronicleQueue queue,
                                              final Wire wire) {

            return ((wire == null) || wire.bytes().readPosition() == readPositionAtLastMove) &&
                    index == this.lastMovedToIndex && index != 0 && state == FOUND_IN_CYCLE &&
                    direction == directionAtLastMoveTo &&
                    queue.rollCycle().toCycle(index) == queue.rollCycle().toCycle(lastMovedToIndex);
        }
    }

    private class Finalizer {
        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            warnAndCloseIfNotClosed();
        }
    }

    class StoreTailerContext extends BinaryReadDocumentContext {
        StoreTailerContext() {
            super(null);
        }

        @Override
        public long index() {
            return StoreTailer.this.index();
        }

        @Override
        public int sourceId() {
            return StoreTailer.this.sourceId();
        }

        @Override
        public void close() {
            if (rollbackIfNeeded())
                return;

            if (isPresent() && !isMetaData())
                incrementIndex();

            super.close();
            if (direction == FORWARD)
                setAddress(context.wire() != null);
            else if (direction == BACKWARD)
                setAddress(false);
        }

        boolean present(final boolean present) {
            return this.present = present;
        }

        public void wire(@Nullable final AbstractWire wire) {
            if (wire == this.wire)
                return;

            final AbstractWire oldWire = this.wire;
            this.wire = wire;

            if (oldWire != null)
                oldWire.bytes().release(INIT); // might be held elsewhere if used for another purpose.
        }

        public void metaData(boolean metaData) {
            this.metaData = metaData;
        }
    }
}
