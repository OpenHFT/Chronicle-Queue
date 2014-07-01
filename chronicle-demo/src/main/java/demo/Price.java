package demo;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.constraints.NotNull;

/**
 * A simple container class representing a Price.
 * Note that it must be Serializable so that it can be stored as an object in Chronicle.
 */
public class Price implements BytesMarshallable {
    public String symbol;
    public double bidPrice, askPrice;
    public long bidQuantity, askQuantity;
    public boolean confirmed;

    public Price() {
    }

    public Price(String symbol, double bidPrice, long bidQuantity,double askPrice,  long askQuantity, boolean confirmed) {
        this.symbol = symbol;
        this.bidPrice = bidPrice;
        this.askPrice = askPrice;
        this.bidQuantity = bidQuantity;
        this.askQuantity = askQuantity;
        this.confirmed = confirmed;
    }

    @Override
    public void writeMarshallable(@NotNull Bytes out) {
        out.writeEnum(symbol);
        out.writeCompactDouble(bidPrice);
        out.writeCompactDouble(askPrice);
        out.writeCompactLong(bidQuantity);
        out.writeCompactLong(askQuantity);
        out.writeBoolean(confirmed);

    }

    @Override
    public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
        symbol = in.readEnum(String.class);
        bidPrice = in.readCompactDouble();
        askPrice = in.readCompactDouble();
        bidQuantity = in.readCompactLong();
        askQuantity = in.readCompactLong();
        confirmed = in.readBoolean();
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public double getBidPrice() {
        return bidPrice;
    }

    public void setBidPrice(double bidPrice) {
        this.bidPrice = bidPrice;
    }

    public double getAskPrice() {
        return askPrice;
    }

    public void setAskPrice(double askPrice) {
        this.askPrice = askPrice;
    }

    public long getBidQuantity() {
        return bidQuantity;
    }

    public void setBidQuantity(long bidQuantity) {
        this.bidQuantity = bidQuantity;
    }

    public long getAskQuantity() {
        return askQuantity;
    }

    public void setAskQuantity(long askQuantity) {
        this.askQuantity = askQuantity;
    }

    public boolean isConfirmed() {
        return confirmed;
    }

    public void setConfirmed(boolean confirmed) {
        this.confirmed = confirmed;
    }
}