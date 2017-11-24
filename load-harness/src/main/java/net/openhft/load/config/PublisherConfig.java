package net.openhft.load.config;

import java.nio.file.Path;

public final class PublisherConfig {
    private final Path outputDir;
    private final int publishRateMegaBytesPerSecond;
    private final int stageCount;

    public PublisherConfig(final Path outputDir, final int publishRateMegaBytesPerSecond, final int stageCount) {
        this.outputDir = outputDir;
        this.publishRateMegaBytesPerSecond = publishRateMegaBytesPerSecond;
        this.stageCount = stageCount;
    }

    public Path outputDir() {
        return outputDir;
    }

    public int getPublishRateMegaBytesPerSecond() {
        return publishRateMegaBytesPerSecond;
    }
}
