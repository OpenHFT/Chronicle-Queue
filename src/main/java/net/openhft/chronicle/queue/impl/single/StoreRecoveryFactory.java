package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.wire.WireType;

import java.util.function.Function;

/**
 * Created by peter on 22/05/16.
 */
@FunctionalInterface
public interface StoreRecoveryFactory extends Function<WireType, StoreRecovery> {
}
