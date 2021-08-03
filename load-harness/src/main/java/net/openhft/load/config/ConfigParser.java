package net.openhft.load.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public final class ConfigParser {
    private final PublisherConfig config;
    private final List<StageConfig> stageConfigList = new ArrayList<>();
    private final int pretouchIntervalMillis;

    public ConfigParser(final String resourceName) throws IOException {
        final Properties properties = new Properties();
        try (final InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName)) {
            if (stream == null) {
                throw new IllegalArgumentException("Resource not found: " + resourceName);
            }
            properties.load(stream);
        }
        final int stageCount = requiredIntValue(properties, "stage.count");
        this.config = new PublisherConfig(
                toRelativePath(requiredValue(properties, "publisher.outputDir")),
                requiredIntValue(properties, "publisher.rate.mbps"), stageCount);

        for (int i = 0; i < stageCount; i++) {
            stageConfigList.add(parseStageConfig(properties, i));
        }

        final Set<Integer> allIndices = new HashSet<>();
        for (StageConfig stageConfig : stageConfigList) {
            for (Integer integer : stageConfig.getStageIndices()) {
                if (!allIndices.add(integer)) {
                    throw new IllegalArgumentException("Duplicate stage index found: " + integer);
                }
            }
        }

        pretouchIntervalMillis = requiredIntValue(properties, "pretouch.interval.ms");
    }

    private static Path toRelativePath(final String subDir) {
        return Paths.get(System.getProperty("user.dir"), subDir);
    }

    private static int requiredIntValue(final Properties properties, final String key) {
        final String value = requiredValue(properties, key);
        try {
            return Integer.parseInt(value);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Cannot parse " + value + " as int for key " + key);
        }
    }

    private static String requiredValue(final Properties properties, final String key) {
        if (System.getProperties().containsKey(key)) {
            return System.getProperties().getProperty(key);
        }
        if (!properties.containsKey(key)) {
            throw new IllegalArgumentException("Cannot find required key: " + key);
        }
        return properties.getProperty(key);
    }

    public PublisherConfig getPublisherConfig() {
        return config;
    }

    public StageConfig getStageConfig(final int index) {
        return stageConfigList.get(index);
    }

    public List<StageConfig> getAllStageConfigs() {
        return stageConfigList;
    }

    public int getPretouchIntervalMillis() {
        return pretouchIntervalMillis;
    }

    private StageConfig parseStageConfig(final Properties properties, final int index) {
        final int consumerCount = requiredIntValue(properties, String.format("stage.%d.consumers", index));
        final List<Integer> stageIndices = new ArrayList<>(consumerCount);
        for (int i = 0; i < consumerCount; i++) {
            stageIndices.add(requiredIntValue(properties, String.format("stage.%d.consumer.%d.index", index, i)));
        }
        return new StageConfig(
                toRelativePath(requiredValue(properties, String.format("stage.%d.inputDir", index))),
                toRelativePath(requiredValue(properties, String.format("stage.%d.outputDir", index))),
                stageIndices);
    }
}