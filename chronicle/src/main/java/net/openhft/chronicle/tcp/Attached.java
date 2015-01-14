package net.openhft.chronicle.tcp;

import net.openhft.chronicle.MappingFunction;
import net.openhft.chronicle.MappingProvider;

/**
 * Created by Rob Austin
 */
public class Attached implements MappingProvider<Attached> {

    private MappingFunction mappingFunction;

    @Override
    public MappingFunction withMapping() {
        return mappingFunction;
    }

    @Override
    public Attached withMapping(MappingFunction mappingFunction) {
        this.mappingFunction = mappingFunction;
        return this;
    }
}
