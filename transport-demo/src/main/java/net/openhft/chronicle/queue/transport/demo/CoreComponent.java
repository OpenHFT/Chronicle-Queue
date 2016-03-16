package net.openhft.chronicle.queue.transport.demo;

/**
 * Created by peter on 16/03/16.
 */
public class CoreComponent implements IUpstream {

    @Override
    public void onPrice(Price price) {
        throw new UnsupportedOperationException();
    }
}
