package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.wire.WireKey;

/**
 * Created by Peter on 05/03/2016.
 */
public enum MetaDataKeys implements WireKey {
    header,
    index2index,
    index,
    roll
}
