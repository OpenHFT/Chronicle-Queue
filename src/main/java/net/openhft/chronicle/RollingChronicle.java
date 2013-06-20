package net.openhft.chronicle;

import java.io.IOException;

/**
 * @author peter.lawrey
 */
public class RollingChronicle implements Chronicle {
    static final int LINE_SIZE = 64;
    static final int DATA_BLOCK_SIZE = 128 * 1024 * 1024;
    static final int INDEX_BLOCK_SIZE = DATA_BLOCK_SIZE / 4;

    protected final MappedFileCache fileCache;
    private final ChronicleConfig config;
    private final RollingNativeExcerptAppender appender;

    public RollingChronicle(String dirPath, ChronicleConfig config) throws IOException {
        this(getMappedFileCache(dirPath, config), config);
    }

    private static MappedFileCache getMappedFileCache(String dirPath, ChronicleConfig config) throws IOException {
        return config.minimiseFootprint()
                ? new LightMappedFileCache(dirPath, config)
                : new VanillaMappedFileCache(dirPath, config);
    }

    public RollingChronicle(MappedFileCache mappedFileCache, ChronicleConfig config) throws IOException {
        fileCache = mappedFileCache;
        this.config = config.clone();
        appender = new RollingNativeExcerptAppender(this);
    }

    @Override
    public ExcerptReader createReader() {
        fileCache.randomAccess(true);
        return new RollingNativeExcerptReader(this);
    }

    @Override
    public ExcerptTailer createTailer() {
        return new RollingNativeExcerptTailer(this);
    }

    @Override
    public ExcerptAppender createAppender() {
        return appender;
    }

    @Override
    public long size() {
        return appender.size();
    }

    @Override
    public void close() throws IOException {
        fileCache.close();
    }
}
