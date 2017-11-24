package net.openhft.load.config;

import java.nio.file.Path;
import java.util.List;

public final class StageConfig {
    private final Path inputPath;
    private final Path outputPath;
    private final List<Integer> stageIndices;

    public StageConfig(final Path inputPath, final Path outputPath, final List<Integer> stageIndices) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.stageIndices = stageIndices;
    }

    public Path getInputPath() {
        return inputPath;
    }

    public Path getOutputPath() {
        return outputPath;
    }

    public List<Integer> getStageIndices() {
        return stageIndices;
    }
}