package net.openhft.chronicle.queue.micros;

/**
 * Created by peter on 22/03/16.
 */
public interface SidedMarketDataListener {
    void onSidedPrice(SidedPrice sidedPrice);
}
