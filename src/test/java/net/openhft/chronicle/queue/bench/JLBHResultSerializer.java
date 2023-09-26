package net.openhft.chronicle.queue.bench;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.jlbh.JLBHResult;
import net.openhft.chronicle.wire.TextWire;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Optional;

public class JLBHResultSerializer {
    public static void runResultToCSV(JLBHResult jlbhResult, String fileName) throws IOException {
        Bytes<?> bytes3 = Bytes.allocateElasticOnHeap();
        TextWire wire3 = new TextWire(bytes3);
        writeHeader(wire3);

        JLBHResult.ProbeResult probeResult = jlbhResult.endToEnd();

        writeProbeResultRows(probeResult, wire3, "endToEnd");
        writeProbeResult(jlbhResult, wire3, "Concurrent");
        writeProbeResult(jlbhResult, wire3, "Concurrent2");

        bytes3.copyTo(Files.newOutputStream(Paths.get(fileName)));
    }


    private static void writeProbeResult(JLBHResult jlbhResult, TextWire wire3, String probeName) {
        Optional<JLBHResult.ProbeResult> probeResult = jlbhResult.probe(probeName);
        probeResult.ifPresent(probeResult1 -> writeProbeResultRows(probeResult1, wire3, probeName));
    }

    private static void writeProbeResultRows(JLBHResult.ProbeResult probeResult, TextWire wire3, String probeName) {
        JLBHResult.@NotNull RunResult runResult = probeResult.summaryOfLastRun();
        writeRow(probeName, wire3, runResult);

//        List<JLBHResult.RunResult> runResults = probeResult.eachRunSummary();
//        for (int i = 0; i < runResults.size(); i++) {
//            JLBHResult.RunResult runResult = runResults.get(i);
//            writeRow(probeName + "-" + i, wire3, runResult);
//        }
    }

    private static void writeRow(String probeName, TextWire wire3, JLBHResult.RunResult runResult) {
        writeValue(wire3, probeName);
        writeValue(wire3, runResult.get50thPercentile());
        writeValue(wire3, runResult.get90thPercentile());
        writeValue(wire3, runResult.get99thPercentile());
        writeValue(wire3, runResult.get999thPercentile());
        writeValue(wire3, runResult.get9999thPercentile());
        wire3.append("\n");
    }

    private static void writeHeader(TextWire wire3) {
        writeValue(wire3, "");
        writeValue(wire3, "50th p-le");
        writeValue(wire3, "90th p-le");
        writeValue(wire3, "99th p-le");
        writeValue(wire3, "999th p-le");
        writeValue(wire3, "9999th p-le");
        wire3.append("\n");

    }

    private static void writeValue(TextWire wire3, Duration runResult) {
        if (runResult != null) {
            wire3.append(Integer.toString(runResult.getNano()));
        }
        wire3.append(",");
    }

    private static void writeValue(TextWire wire3, String runResult) {
        wire3.append(runResult);
        wire3.append(",");
    }
}
