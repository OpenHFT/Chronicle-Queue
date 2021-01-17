package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.core.util.Histogram;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public interface HistoryReader {

    HistoryReader withMessageSink(final Consumer<String> messageSink);

    HistoryReader withBasePath(final Path path);

    HistoryReader withProgress(boolean p);

    HistoryReader withTimeUnit(TimeUnit p);

    HistoryReader withHistosByMethod(boolean b);

    HistoryReader withIgnore(long ignore);

    HistoryReader withMeasurementWindow(long measurementWindow);

    HistoryReader withSummaryOutput(int offset);

    void execute();

    Map<String, Histogram> readChronicle();

    void outputData();
}