package net.openhft.chronicle.queue.example;

import net.openhft.chronicle.queue.ChronicleQueue;

public class QueueExamples1 {

    public static void main(String[] args) {

        ChronicleQueue queue = ChronicleQueue.single("./myQueueDir");
        Printer printer = queue.methodWriter(Printer.class);
        printer.print("hello world");
    }

    // this interface has to be deployed to both java processes
    interface Printer {
        void print(String message);
    }

}
