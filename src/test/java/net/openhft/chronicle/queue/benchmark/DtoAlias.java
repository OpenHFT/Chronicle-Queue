package net.openhft.chronicle.queue.benchmark;

import net.openhft.chronicle.core.pool.ClassAliasPool;

public enum DtoAlias {
    INSTANCE;

    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(Rung.class,
                BookUpdate.class);
    }

    public static void init() {

    }
}
