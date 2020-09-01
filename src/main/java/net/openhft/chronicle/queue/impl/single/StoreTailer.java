package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
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
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.EOFException;
import java.io.StreamCorruptedException;
import java.text.ParseException;

import static net.openhft.chronicle.bytes.NoBytesStore.NO_PAGE;
import static net.openhft.chronicle.core.UnsafeMemory.UNSAFE;
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
    private final LongValue indexValue;
    private final StoreTailerContext context = new StoreTailerContext();
    private final MoveToState moveToState = new MoveToState();
    long index; // index of the next read.
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
    private final Finalizer finalizer;
    private boolean disableThreadSafetyCheck;

    public StoreTailer(@NotNull final SingleChronicleQueue queue) {
        this(queue, null);
    }

    public StoreTailer(@NotNull final SingleChronicleQueue queue, final LongValue indexValue) {
        this.queue = queue;
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
    }

    @Override
    public ExcerptTailer disableThreadSafetyCheck(boolean disableThreadSafetyCheck) {
        this.disableThreadSafetyCheck = disableThreadSafetyCheck;
        return this;
    }

    @Override
    protected boolean threadSafetyCheck(boolean isUsed) {
        return disableThreadSafetyCheck
                || super.threadSafetyCheck(isUsed);
    }

    private class Finalizer {
        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            warnAndCloseIfNotClosed();
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
            w0.bytes().releaseLast();
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
        throwExceptionIfClosed();

        try {
//            Jvm.optionalSafepoint();
            boolean next = false, tryAgain = true;
            if (state == FOUND_CYCLE) {
                try {
//                    Jvm.optionalSafepoint();
                    next = inACycle(includeMetaData);
//                    Jvm.optionalSafepoint();

                    tryAgain = false;
                } catch (EOFException eof) {
                    state = TailerState.END_OF_CYCLE;
                }
            }
//            Jvm.optionalSafepoint();

            if (tryAgain)
                next = next0(includeMetaData);

//            Jvm.optionalSafepoint();
            if (context.present(next)) {
                Bytes<?> bytes = context.wire().bytes();
                context.setStart(bytes.readPosition() - 4);
                readingDocumentFound = true;
                address = bytes.addressForRead(bytes.readPosition(), 4);
//                Jvm.optionalSafepoint();
                return context;
            }
//            Jvm.optionalSafepoint();

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
                Jvm.warn().on(StoreTailer.class, "Tried to read past the end of a read-only view. " +
                        "Underlying data store may have grown since this tailer was created.", e);
            } else {
                throw e;
            }
        }
        return INSTANCE;
    }

    @SuppressWarnings("restriction")
    @Override
    public boolean peekDocument() {
        throwExceptionIfClosed();

        if (address == NO_PAGE || state != FOUND_CYCLE || direction != FORWARD)
            return peekDocument0();

        final int header = UNSAFE.getIntVolatile(null, address);

        if (header == END_OF_DATA)
            return peekDocument0();

        return header > 0x0;
    }

    private boolean peekDocument0() {
        try (DocumentContext dc = readingDocument()) {
            dc.rollbackOnClose();
            return dc.isPresent();
        }
    }

    private boolean next0(final boolean includeMetaData) throws UnrecoverableTimeoutException, StreamCorruptedException {
        for (int i = 0; i < 1000; i++) {
            switch (state) {
                case UNINITIALISED:
                    final long firstIndex = queue.firstIndex();
                    if (firstIndex == Long.MAX_VALUE)
                        return false;
                    if (!moveToIndexInternal(firstIndex))
                        return false;
                    break;

                case FOUND_CYCLE: {
                    try {
                        return inACycle(includeMetaData);
                    } catch (EOFException eof) {
                        state = TailerState.END_OF_CYCLE;
                    }
                    break;
                }

                case END_OF_CYCLE:
                    if (endOfCycle()) {
//                        Jvm.optionalSafepoint();
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
            return nextEndOfCycle(nextIndex);
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

    private boolean nextEndOfCycle(final long nextIndex) {
        if (moveToIndexInternal(nextIndex)) {
            state = FOUND_CYCLE;
//            Jvm.optionalSafepoint();
            return true;
        }
//        Jvm.optionalSafepoint();
        if (state == END_OF_CYCLE) {
//            Jvm.optionalSafepoint();
            return true;
        }
        if (cycle < queue.lastCycle()) {
            // we have encountered an empty file without an EOF marker
            state = END_OF_CYCLE;
//            Jvm.optionalSafepoint();
            return true;
        }
        // We are here because we are waiting for an entry to be written to this file.
        // Winding back to the previous cycle results in a re-initialisation of all the objects => garbage
        int nextCycle = queue.rollCycle().toCycle(nextIndex);
        cycle(nextCycle);
        state = CYCLE_NOT_FOUND;
        return false;
    }

    private boolean beyondStartOfCycleBackward() throws StreamCorruptedException {
        // give the position of the last entry and
        // flag we want to count it even though we don't know if it will be meta data or not.

        final boolean foundCycle = cycle(queue.rollCycle().toCycle(index()));

        if (foundCycle) {
            final long lastSequenceNumberInThisCycle = store().sequenceForPosition(this, Long.MAX_VALUE, false);
            final long nextIndex = queue.rollCycle().toIndex(this.cycle, lastSequenceNumberInThisCycle);
            moveToIndexInternal(nextIndex);
            state = FOUND_CYCLE;
            return true;
        }

        final int cycle = queue.rollCycle().toCycle(index());
        final long nextIndex = nextIndexWithNextAvailableCycle(cycle);

        if (nextIndex != Long.MIN_VALUE) {
            moveToIndexInternal(nextIndex);
            state = FOUND_CYCLE;
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
            state = FOUND_CYCLE;
            return true;
        }
        return false;
    }

    private boolean inACycle(final boolean includeMetaData) throws EOFException {
//        Jvm.optionalSafepoint();
        if (readAfterReplicaAcknowledged && inACycleCheckRep()) return false;

//        Jvm.optionalSafepoint();
        if (direction != TailerDirection.FORWARD && !inACycleNotForward()) return false;
//        Jvm.optionalSafepoint();

        final Wire wire = privateWire();
        final Bytes<?> bytes = wire.bytes();
        return inACycle2(includeMetaData, wire, bytes);
    }

    private boolean inACycle2(boolean includeMetaData, Wire wire, Bytes<?> bytes) throws EOFException {
        bytes.readLimitToCapacity();

        switch (wire.readDataHeader(includeMetaData)) {
            case NONE:
//                Jvm.optionalSafepoint();
                // no more polling - appender will always write (or recover) EOF
                return false;
            case META_DATA:
//                Jvm.optionalSafepoint();
                context.metaData(true);
                break;
            case DATA:
//                Jvm.optionalSafepoint();
                context.metaData(false);
                break;
            case EOF:
                throw EOF_EXCEPTION;
        }

//        Jvm.optionalSafepoint();
        inACycleFound(bytes);
//        Jvm.optionalSafepoint();
        return true;
    }

    private boolean inACycleCheckRep() {
        final long lastSequenceAck = queue.lastAcknowledgedIndexReplicated();
        final long index = index();
        return index > lastSequenceAck;
    }

    private boolean inACycleNotForward() {
//        Jvm.optionalSafepoint();
        if (!moveToIndexInternal(index())) {
            try {
//                Jvm.optionalSafepoint();
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
//                    Jvm.optionalSafepoint();
                    return false;
                }
            } catch (Exception e) {
                // can happen if index goes negative
//                Jvm.optionalSafepoint();
                return false;
            }
        }
//        Jvm.optionalSafepoint();
        return true;
    }

    private void inACycleFound(@NotNull final Bytes<?> bytes) {
        context.closeReadLimit(bytes.capacity());
        privateWire().readAndSetLength(bytes.readPosition());
        final long end = bytes.readLimit();
        context.closeReadPosition(end);
//        Jvm.optionalSafepoint();
    }

    private long nextIndexWithNextAvailableCycle(final int cycle) {
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
        state = FOUND_CYCLE;
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

    private ScanResult moveToIndexResult0(final long index) {

        final int cycle = queue.rollCycle().toCycle(index);
        final long sequenceNumber = queue.rollCycle().toSequenceNumber(index);
//        if (Jvm.isResourceTracing()) {
//            Jvm.debug().on(getClass(), "moveToIndex: " + Long.toHexString(cycle) + " " + Long.toHexString(sequenceNumber));
//        }

        if (cycle != this.cycle || state != FOUND_CYCLE) {
            // moves to the expected cycle
            if (!cycle(cycle))
                return ScanResult.NOT_REACHED;
        }

        index(index);
        final ScanResult scanResult = this.store().moveToIndexForRead(this, sequenceNumber);
        final Bytes<?> bytes = privateWire().bytes();
        if (scanResult == FOUND) {
            state = FOUND_CYCLE;
            moveToState.onSuccessfulLookup(index, direction, bytes.readPosition());
            return scanResult;
        } else if (scanResult == END_OF_FILE) {
            state = END_OF_CYCLE;
            return scanResult;
        } else if (scanResult == NOT_FOUND && this.cycle < this.queue.lastCycle) {
            state = END_OF_CYCLE;
            return END_OF_FILE;
        }

        return scanResult;

    }

    ScanResult moveToIndexResult(final long index) {
        final ScanResult scanResult = moveToIndexResult0(index);
        setAddress(scanResult == FOUND);
        return scanResult;
    }

    @NotNull
    @Override
    public final ExcerptTailer toStart() {
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
            assert found || store == null;
            if (found)
                state = FOUND_CYCLE;
        }
        index(queue.rollCycle().toIndex(cycle, 0));

        state = FOUND_CYCLE;
        Wire wire = privateWire();
        if (wire != null) {
            wire.bytes().readPosition(0);
            address = wire.bytes().addressForRead(0);
        }
        return this;
    }

    private boolean moveToIndexInternal(final long index) {
        moveToState.indexMoveCount++;
//        Jvm.optionalSafepoint();
        final ScanResult scanResult = moveToIndexResult(index);
//        Jvm.optionalSafepoint();
        return scanResult == FOUND;
    }

    /**
     * gives approximately the last index, can not be relied on as the last index may have changed just after this was called. For this reason,
     * this code is not in queue as it should only be an internal method
     *
     * @return the last index at the time this method was called, or Long.MIN_VALUE if none.
     */
    private long approximateLastIndex() {

        final RollCycle rollCycle = queue.rollCycle();
        final int lastCycle = queue.lastCycle();
        try {
            if (lastCycle == Integer.MIN_VALUE)
                return Long.MIN_VALUE;

            final SingleChronicleQueueStore wireStore = queue.storeForCycle(
                    lastCycle, queue.epoch(), false, this.store);
            this.setCycle(lastCycle);
            if (wireStore == null)
                throw new IllegalStateException("Store not found for cycle " + Long.toHexString(lastCycle) + ". Probably the files were removed?");

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
                return rollCycle.toIndex(lastCycle, 0L);
            }
            return rollCycle.toIndex(lastCycle, sequenceNumber);

        } catch (@NotNull StreamCorruptedException | UnrecoverableTimeoutException e) {
            throw new IllegalStateException(e);
        }
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

        final AbstractWire wire = (AbstractWire) readAnywhere(wireType.apply(store.bytes()));
        assert !SingleChronicleQueue.CHECK_INDEX || headerNumberCheck(wire);
        this.context.wire(wire);
        wire.parent(this);

        final Wire wireForIndexOld = wireForIndex;
        wireForIndex = readAnywhere(wireType.apply(store().bytes()));
        assert !SingleChronicleQueue.CHECK_INDEX || headerNumberCheck((AbstractWire) wireForIndex);
        assert wire != wireForIndexOld;

        if (wireForIndexOld != null)
            wireForIndexOld.bytes().releaseLast();
    }

    @NotNull
    private Wire readAnywhere(@NotNull final Wire wire) {
        final Bytes<?> bytes = wire.bytes();
        bytes.readLimitToCapacity();
        if (store.dataVersion() > 0)
            wire.usePadding(true);
        return wire;
    }

    @NotNull
    @Override
    public ExcerptTailer toEnd() {
        throwExceptionIfClosed();

        //  if (direction.equals(TailerDirection.BACKWARD))

        try{
            return originalToEnd();
        }   catch (NotReachedException e) {
            // due to a race condition, where the queue rolls as we are processing toEnd()
            // we may get a NotReachedException  ( see https://github.com/OpenHFT/Chronicle-Queue/issues/702 )
            // hence are are just going to retry.
            return originalToEnd();
        }

        //  todo fix -see issue https://github.com/OpenHFT/Chronicle-Queue/issues/689
        // return optimizedToEnd();
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
                setAddress(state == FOUND_CYCLE);
                return this;
            }

            final SingleChronicleQueueStore wireStore = queue.storeForCycle(
                    lastCycle, queue.epoch(), false, this.store);
            this.setCycle(lastCycle);
            if (wireStore == null)
                throw new IllegalStateException("Store not found for cycle " + Long.toHexString(lastCycle) + ". Probably the files were removed? lastCycle=" + lastCycle);

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
                // nothing has been written yet, so point to start of cycle
                return originalToEnd();
            }

            final Bytes<?> bytes = privateWire().bytes();
            state = isEndOfFile(bytes.readVolatileInt(bytes.readPosition())) ? END_OF_CYCLE : FOUND_CYCLE;

            index(rollCycle.toIndex(lastCycle, sequenceNumber));

            setAddress(state == FOUND_CYCLE);
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
        final ScanResult scanResult = moveToIndexResult(index);
        switch (scanResult) {
            case NOT_FOUND:
                if (moveToIndexResult(index - 1) == FOUND)
                    state = FOUND_CYCLE;
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
                            state = FOUND_CYCLE;
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
        long seq = rollCycle.toSequenceNumber(index);
        final int cycle = rollCycle.toCycle(index);

        seq += direction.add();
        switch (direction) {
            case NONE:
                break;
            case FORWARD:
                // if it runs out of seq number it will flow over to tomorrows cycle file
                if (rollCycle.toSequenceNumber(seq) < seq) {
                    cycle(cycle + 1);
                    Jvm.warn().on(getClass(),
                            "we have run out of sequence numbers, so will start to write to " +
                                    "the next .cq4 file, the new cycle=" + cycle);
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
        final long count = queue.exceptsPerCycle(cycle);
        if (count <= 0)
            return false;
        final RollCycle rollCycle = queue.rollCycle();
        moveToIndexInternal(rollCycle.toIndex(cycle, count - 1));
        this.state = FOUND_CYCLE;
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
        if (this.cycle == cycle && state == FOUND_CYCLE)
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
        state = FOUND_CYCLE;
        setCycle(cycle);
        resetWires();
        final Wire wire = privateWire();
        wire.parent(this);
        wire.pauser(queue.pauserSupplier.get());
        return true;
    }

    void releaseStore() {
        if (store != null) {
            store.close();
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
        throwExceptionIfClosedInSetter();

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

    private static final class MoveToState {
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
                    state == FOUND_CYCLE &&
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
                    index == this.lastMovedToIndex && index != 0 && state == FOUND_CYCLE &&
                    direction == directionAtLastMoveTo &&
                    queue.rollCycle().toCycle(index) == queue.rollCycle().toCycle(lastMovedToIndex);
        }
    }

    class StoreTailerContext extends BinaryReadDocumentContext {

        boolean rollbackOnClose = false;

        StoreTailerContext() {
            super(null);
        }

        @Override
        public void rollbackOnClose() {
            rollbackOnClose = true;
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
            try {
                if (rollbackOnClose) {
                    present = false;
                    if (start > -1)
                        wire.bytes().readPosition(start).readLimit(readLimit);
                    start = -1;
                    return;
                }

                if (isPresent() && !isMetaData())
                    incrementIndex();

                super.close();
                if (direction == FORWARD)
                    setAddress(context.wire() != null);
                else if (direction == BACKWARD)
                    setAddress(false);

            } finally {
                rollbackOnClose = false;
            }
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
    }

}
