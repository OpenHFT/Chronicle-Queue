package net.openhft.chronicle;

import net.openhft.lang.io.Bytes;

import java.io.Serializable;

/**
 * Represents a function that accepts one argument and produces a result.
 */
public interface MappingFunction extends Serializable {

    public void apply(Bytes from, Bytes to);

}
