package net.openhft.chronicle.queue.reader.comparator;

import net.openhft.chronicle.queue.reader.Reader;
import net.openhft.chronicle.wire.Wire;

import java.util.Comparator;
import java.util.function.Consumer;

public interface BinarySearchComparator extends Comparator<Wire>, Consumer<Reader> {
    Wire wireKey();
}
