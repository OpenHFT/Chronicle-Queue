package net.openhft.chronicle.queue.rollcycles;

import net.openhft.chronicle.queue.RollCycle;

/**
 * These are used to minimise rolls but do create very large files, possibly too large.
 */
public enum LargeRollCycles implements RollCycle {
    /**
     * 0xffffffff entries per hour, indexing every 64th entry
     */
    LARGE_HOURLY(/*----*/"yyyyMMdd-HH'L'", 60 * 60 * 1000, 8 << 10, 64),
    /**
     * 0x1fffffffff entries per day, indexing every 128th entry
     */
    LARGE_DAILY(/*-----*/"yyyyMMdd'L'", 24 * 60 * 60 * 1000, MAX_INDEX_COUNT, 128),
    /**
     * 0x3ffffffffff entries per day, indexing every 256th entry
     */
    XLARGE_DAILY(/*----*/"yyyyMMdd'X'", 24 * 60 * 60 * 1000, MAX_INDEX_COUNT, 256),
    /**
     * 0xffffffffffff entries per day with sparse indexing (every 1024th entry)
     */
    HUGE_DAILY(/*------*/"yyyyMMdd'H'", 24 * 60 * 60 * 1000, MAX_INDEX_COUNT, 1024),
    ;

    private final String format;
    private final int lengthInMillis;
    private final RollCycleArithmetic arithmetic;

    LargeRollCycles(String format, int lengthInMillis, int indexCount, int indexSpacing) {
        this.format = format;
        this.lengthInMillis = lengthInMillis;
        this.arithmetic = RollCycleArithmetic.of(indexCount, indexSpacing);
    }

    public long maxMessagesPerCycle() {
        return arithmetic.maxMessagesPerCycle();
    }

    @Override
    public String format() {
        return this.format;
    }

    @Override
    public int lengthInMillis() {
        return this.lengthInMillis;
    }

    /**
     * @return this is the size of each index array, note: indexCount^2 is the maximum number of index queue entries.
     */
    @Override
    public int defaultIndexCount() {
        return arithmetic.indexCount();
    }

    @Override
    public int defaultIndexSpacing() {
        return arithmetic.indexSpacing();
    }

    @Override
    public long toIndex(int cycle, long sequenceNumber) {
        return arithmetic.toIndex(cycle, sequenceNumber);
    }

    @Override
    public long toSequenceNumber(long index) {
        return arithmetic.toSequenceNumber(index);
    }

    @Override
    public int toCycle(long index) {
        return arithmetic.toCycle(index);
    }
}
