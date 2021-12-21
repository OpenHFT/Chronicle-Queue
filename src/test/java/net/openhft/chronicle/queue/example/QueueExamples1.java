package net.openhft.chronicle.queue.example;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

public class QueueExamples1 {

    public static void main(String[] args) {

        ChronicleQueue chronicleQueue = SingleChronicleQueueBuilder.binary("./myQueueDir").build();
        final Printer printer = chronicleQueue.methodWriter(Printer.class);
        printer.print("hello world");
    }

    // this interface has to be deployed to both java processes
    interface Printer {
        void print(String message);
    }

}
