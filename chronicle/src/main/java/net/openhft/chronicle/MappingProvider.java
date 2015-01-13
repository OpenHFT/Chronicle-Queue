package net.openhft.chronicle;

/**
 * @author Rob Austin.
 */
public interface MappingProvider<T> {

    MappingFunction withMapping();

    T withMapping(MappingFunction mappingFunction);
}
