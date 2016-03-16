package net.openhft.chronicle.queue.transport.demo;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.AbstractMarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

/**
 * Created by peter on 16/03/16.
 */
public class Price extends AbstractMarshallable {
    double askPrice, askQuantity, bidPrice, bidQuantity;
    private String instrument;

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
        instrument = wire.read(() -> "in").text();
        askPrice = wire.read(() -> "ap").float64();
        askQuantity = wire.read(() -> "aq").float64();
        bidPrice = wire.read(() -> "bp").float64();
        bidQuantity = wire.read(() -> "bq").float64();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(() -> "in").text(instrument);
        wire.write(() -> "ap").float64(askPrice);
        wire.write(() -> "aq").float64(askQuantity);
        wire.write(() -> "bp").float64(bidPrice);
        wire.write(() -> "bq").float64(bidQuantity);
    }
}
