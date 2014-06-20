package demo;

import java.io.Serializable;
/**
 * A simple container class representing a Price.
 * Note that it must be Serializable so that it can be stored as an object in Chronicle.
 */
public class Price implements Serializable{
    public String symbol;
    public long price;
    public boolean confirmed;

    Price(String symbol, long price, boolean confirmed){
        this.symbol = symbol;
        this.price = price;
        this.confirmed = confirmed;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public long getPrice() {
        return price;
    }

    public void setPrice(long price) {
        this.price = price;
    }

    public boolean isConfirmed() {
        return confirmed;
    }

    public void setConfirmed(boolean confirmed) {
        this.confirmed = confirmed;
    }
}