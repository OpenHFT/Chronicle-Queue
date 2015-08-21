package net.openhft.chronicle.lataeg;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.model.DataValueClasses;

public class ChronicleQueueReader {
    private int noOfRecords = 6000000;
    private int noOfRuns = 10;
    private Chronicle chronicle;

    public static void main(String args[]) throws Exception {
        ChronicleQueueReader reader = new ChronicleQueueReader();
        reader.extractParameters(args);
        reader.createQueue();
        reader.readData();
    }

    private void extractParameters(String args[]) {
        if (args.length >= 1) {
            this.noOfRecords = Integer.valueOf(args[0]);
        }
        System.out.println("No of Records :" + noOfRecords);
        if (args.length > 1) {
            this.noOfRuns = Integer.valueOf(args[1]);
        }
        System.out.println("No of Runs :" + noOfRuns);
    }

    private void createQueue() throws Exception {
        chronicle = ChronicleQueueBuilder.vanilla("./")
                .build();
        System.out.println("Created Queue..");
    }

    public void readData() throws Exception {
        ExcerptTailer tailer = chronicle.createTailer();
        final IChronicleQueueData iChronicleQueueData = DataValueClasses.newDirectReference(IChronicleQueueData.class);
        int run = 0;
        long runEndTime = 0;
        long runStartTime = 0;
        long readTime = 0;
        long nextIndexTime = 0;
        while (run < (noOfRuns)) {
            runStartTime = System.nanoTime();
            int counter = 0;
            long sleepTime = 0;
            nextIndexTime = 0;
            readTime = 0;
            while (counter < noOfRecords) {
                long nextIndexStartTime = System.nanoTime();
                if (tailer.nextIndex()) {
                    long readStartTime = System.nanoTime();
                    nextIndexTime = nextIndexTime + (readStartTime - nextIndexStartTime);
                    iChronicleQueueData.bytes(tailer, 0);
                    iChronicleQueueData.getIntField1();
                    iChronicleQueueData.getIntField2();
                    iChronicleQueueData.getIntField3();
                    iChronicleQueueData.getIntField4();
                    iChronicleQueueData.getIntField5();
                    iChronicleQueueData.getIntField6();
                    iChronicleQueueData.getIntField7();
                    iChronicleQueueData.getIntField8();
                    iChronicleQueueData.getIntField9();
                    iChronicleQueueData.getIntField10();

                    iChronicleQueueData.getIntField11();
                    iChronicleQueueData.getIntField12();
                    iChronicleQueueData.getIntField13();
                    iChronicleQueueData.getIntField14();
                    iChronicleQueueData.getIntField15();
                    iChronicleQueueData.getIntField16();
                    iChronicleQueueData.getIntField17();
                    iChronicleQueueData.getIntField18();
                    iChronicleQueueData.getIntField19();
                    iChronicleQueueData.getIntField20();

                    tailer.finish();
                    long readEndTime = System.nanoTime();
                    readTime = readTime + (readEndTime - readStartTime);
                    counter++;
                } else {
                    Thread.sleep(1);
                    sleepTime++;
                }
            }
            run++;
            runEndTime = System.nanoTime();
            writeToFile("ReadTime:" + run, (runEndTime - runStartTime), nextIndexTime, readTime, 0, sleepTime);
        }
        tailer.close();
    }

    public void writeToFile(String name, long totalTimeTaken, long nextIndexTime, long readTimeTaken, long finishTimeTaken, long sleepTimeTaken) {
        System.out.println(name
                + "," + totalTimeTaken
                + "," + nextIndexTime
                + "," + readTimeTaken
                + "," + finishTimeTaken
                + "," + sleepTimeTaken);
    }

}
