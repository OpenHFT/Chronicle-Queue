package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;

/**
 * Created by peter_2 on 12/02/2016.
 */
public enum NoDocumentContext implements DocumentContext {
    INSTANCE;

    @Override
    public boolean isMetaData() {
        return false;
    }

    @Override
    public boolean isPresent() {
        return false;
    }

    @Override
    public boolean isData() {
        return false;
    }

    @Override
    public Wire wire() {
        return null;
    }

    @Override
    public void start() {

    }

    @Override
    public void close() {

    }
}
