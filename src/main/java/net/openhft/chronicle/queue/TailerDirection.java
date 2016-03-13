package net.openhft.chronicle.queue;

/**
 * Created by Peter on 05/03/2016.
 */
public enum TailerDirection {
    NONE(0), // don't move after a read.
    FORWARD(+1), // move to the next entry
    BACKWARD(-1) // move to the previous entry.
    ;

    private final int add;

    TailerDirection(int add) {
        this.add = add;
    }

    public int add() {
        return add;
    }
}
