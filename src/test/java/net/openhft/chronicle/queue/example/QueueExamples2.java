package net.openhft.chronicle.queue.example;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

public class QueueExamples2 {

    public static void main(String[] args) {

        ChronicleQueue chronicleQueue = SingleChronicleQueueBuilder.binary("./myQueueDir").build();
        final MethodReader methodReader = chronicleQueue.createTailer().methodReader((Printer) System.out::println);

        for (; ; ) {
            final boolean successIfMessageRead = methodReader.readOne();
            Thread.yield();
        }

    }

    // this interface has to be deployed to both java processes
    interface Printer {
        void print(String message);
    }

}