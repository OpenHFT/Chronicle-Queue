package net.openhft.chronicle.queue.benchmark;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;


import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static java.lang.Double.NaN;
import static net.openhft.chronicle.bytes.BytesUtil.triviallyCopyableRange;
import static net.openhft.chronicle.bytes.internal.BytesFieldInfo.lookup;
import static net.openhft.chronicle.core.Jvm.fieldOffset;
import static net.openhft.chronicle.core.UnsafeMemory.UNSAFE;
import static net.openhft.chronicle.wire.Base85LongConverter.INSTANCE;
import static net.openhft.chronicle.wire.Wires.acquireStringBuilder;

/**
 * This is sent every time there is a change on the book that can cause it to reevaluate
 */
public class BookUpdate extends TriviallyCopyableEvent {
    public static final int MAX_NUMBER_OF_BIDS = 9;
    public static final int MAX_NUMBER_OF_ASKS = 9;
    private static final int DESCRIPTION = lookup(BookUpdate.class).description();
    private static final int LENGTH, START;
    private static final long START_OF_BIDS_RATE = fieldOffset(BookUpdate.class, "bidRate0");
    private static final long START_OF_BIDS_QTY = fieldOffset(BookUpdate.class, "bidQty0");
    private static final long START_OF_ASKS_RATE = fieldOffset(BookUpdate.class, "askRate0");
    private static final long START_OF_ASKS_QTY = fieldOffset(BookUpdate.class, "askQty0");

    static {
        final int[] range = triviallyCopyableRange(BookUpdate.class);
        LENGTH = range[1] - range[0];
        START = range[0];
    }


    public boolean usesSelfDescribingMessage() {
        return false;
    }


    @LongConversion(Base85LongConverter.class)
    public long symbol;

    @LongConversion(Base85LongConverter.class)
    public long exchange;

    // bid
    public int bidCount;
    public double bidRate0, bidRate1, bidRate2, bidRate3, bidRate4, bidRate5, bidRate6, bidRate7, bidRate8, bidRate9;
    public double bidQty0, bidQty1, bidQty2, bidQty3, bidQty4, bidQty5, bidQty6, bidQty7, bidQty8, bidQty9;

    // ask
    public int askCount;
    public double askRate0, askRate1, askRate2, askRate3, askRate4, askRate5, askRate6, askRate7, askRate8, askRate9;
    public double askQty0, askQty1, askQty2, askQty3, askQty4, askQty5, askQty6, askQty7, askQty8, askQty9;

    public int bidCount() {
        return bidCount;
    }

    public int askCount() {
        return askCount;
    }

    @Override
    protected int $description() {
        return DESCRIPTION;
    }

    @Override
    protected int $start() {
        return START;
    }

    @Override
    protected int $length() {
        return LENGTH;
    }

    public BookUpdate bidCount(int bidCount) {
        this.bidCount = bidCount;
        return this;
    }

    public BookUpdate askCount(int askCount) {
        this.askCount = askCount;
        return this;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
        eventTime(ServicesTimestampLongConverter.INSTANCE.parse(wire.read("eventTime").text()));
        symbol = readAsLong(wire, "symbol");
        exchange = readAsLong(wire, "exchange");
        bidCount = readRungs(wire, "bid", this::setBidRungs0_9);
        askCount = readRungs(wire, "ask", this::setAskRungs0_9);
    }

    private int readRungs(@NotNull WireIn wire, final String side, Consumer<List<Rung>> setRungs) {
        final List<Rung> rungs = new ArrayList<>();
        wire.read(side + "s").sequence(this, (t, v) -> {
            rungs.clear();
            while (v.hasNextSequenceItem()) {
                Rung r = new Rung();
                v.marshallable(r);
                rungs.add(r);
            }
        });
        setRungs.accept(rungs);
        return rungs.size();
    }


    public double getBidRate(int index) {
        return getDoubleAtOffsetIndex(index, +START_OF_BIDS_RATE);
    }


    public double getBidQty(int index) {
        return getDoubleAtOffsetIndex(index, START_OF_BIDS_QTY);
    }


    public double getAskRate(int index) {
        return getDoubleAtOffsetIndex(index, START_OF_ASKS_RATE);
    }


    public double getAskQty(int index) {
        return getDoubleAtOffsetIndex(index, START_OF_ASKS_QTY);
    }


    private double getDoubleAtOffsetIndex(int index, final long address) {
        return UNSAFE.getDouble(this, address + (index << 3));
    }

    private BookUpdate setDoubleAtOffsetIndex(int index, double rate, final long address) {
        UNSAFE.putDouble(this, address + (index << 3), rate);
        return this;
    }

    public BookUpdate setBidRate(int index, double rate) {
        return setDoubleAtOffsetIndex(index, rate, START_OF_BIDS_RATE);
    }

    public BookUpdate setBidQty(int index, double qty) {
        return setDoubleAtOffsetIndex(index, qty, START_OF_BIDS_QTY);
    }

    public BookUpdate setAskRate(int index, double rate) {
        return setDoubleAtOffsetIndex(index, rate, START_OF_ASKS_RATE);
    }

    public BookUpdate setAskQty(int index, double qty) {
        return setDoubleAtOffsetIndex(index, qty, START_OF_ASKS_QTY);
    }

    public BookUpdate symbol(CharSequence text) {
        this.symbol = INSTANCE.parse(text);
        return this;
    }

    public BookUpdate exchange(CharSequence text) {
        this.exchange = INSTANCE.parse(text);
        return this;
    }

    public long exchange() {
        return this.exchange;
    }

    public long symbol() {
        return this.symbol;
    }

    private void setAskRungs0_9(Iterable<Rung> rungs) {
        int i = 0;
        for (Rung rung : rungs) {
            setAskRate(i, rung.rate).setAskQty(i, rung.qty);
            i++;
        }
        for (; i <= 9; i++) {
            setAskRate(i, NaN).setAskQty(i, 0);
        }
    }


    private void setBidRungs0_9(Iterable<Rung> rungs) {
        int i = 0;
        for (Rung rung : rungs) {
            setDoubleAtOffsetIndex(i, rung.rate, START_OF_BIDS_RATE).setBidQty(i, rung.qty);
            i++;
        }
        for (; i <= 9; i++) {
            setDoubleAtOffsetIndex(i, NaN, START_OF_BIDS_RATE).setBidQty(i, 0);
        }
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write("eventTime").text(ServicesTimestampLongConverter.INSTANCE.asString(eventTime()));
        wire.write("symbol").text(INSTANCE.asText(symbol));
        wire.write("exchange").text(INSTANCE.asText(exchange));
        wire.write("bids").list(addBidRungs(bidCount), Rung.class);
        wire.write("asks").list(addAskRungs(askCount), Rung.class);
    }

    private List<Rung> addBidRungs(int count) {
        final List<Rung> rungs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            rungs.add(new Rung(getBidRate(i), getBidQty(i)));
        }
        return rungs;
    }

    private List<Rung> addAskRungs(int count) {
        final List<Rung> rungs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            rungs.add(new Rung(getAskRate(i), getAskQty(i)));
        }
        return rungs;
    }

    private static long readAsLong(@NotNull WireIn wire, final String symbol) {
        final StringBuilder sb = acquireStringBuilder();
        wire.read(symbol).text(sb);
        return INSTANCE.parse(sb);
    }


    @Override
    public void reset() {
        super.reset();
        bidCount = 0;
        askCount = 0;
    }
}
