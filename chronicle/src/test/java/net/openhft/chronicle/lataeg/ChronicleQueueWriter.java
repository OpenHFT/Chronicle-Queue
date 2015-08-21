package net.openhft.chronicle.lataeg;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.lang.model.DataValueClasses;

public class ChronicleQueueWriter {
    private int noOfRecords = 6000000;
    private int noOfRuns = 10;
    private Chronicle chronicle;

    public static void main(String args[]) throws Exception {
        ChronicleQueueWriter writer = new ChronicleQueueWriter();
        writer.extractParameters(args);
        writer.generateData();
        writer.createQueue();
        writer.writeDataToQueue();
    }

    private void generateData() {
        System.out.println("Generated Data..");
    }

    private void extractParameters(String[] args) {
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
        chronicle = ChronicleQueueBuilder.vanilla("/tmp/indexed").build();
        System.out.println("Created Queue..");
    }

    private void writeDataToQueue() throws Exception {
        ExcerptAppender appender = chronicle.createAppender();
        System.out.println("Starting to write data to queue..");
        final IChronicleQueueData iChronicleQueueData = DataValueClasses.newDirectInstance(IChronicleQueueData.class);
        IChronicleQueueData chronicleQueueData = DataValueClasses.newInstance(IChronicleQueueData.class);
        for (int i = 0; i < noOfRuns; i++) {
            long startTime = System.nanoTime();
            for (int j = 0; j < noOfRecords; j++) {
                iChronicleQueueData.setIntField1(chronicleQueueData.getIntField1());
                iChronicleQueueData.setIntField2(chronicleQueueData.getIntField2());
                iChronicleQueueData.setIntField3(chronicleQueueData.getIntField3());
                iChronicleQueueData.setIntField4(chronicleQueueData.getIntField4());
                iChronicleQueueData.setIntField5(chronicleQueueData.getIntField5());
                iChronicleQueueData.setIntField6(chronicleQueueData.getIntField6());
                iChronicleQueueData.setIntField7(chronicleQueueData.getIntField7());
                iChronicleQueueData.setIntField8(chronicleQueueData.getIntField8());
                iChronicleQueueData.setIntField9(chronicleQueueData.getIntField9());
                iChronicleQueueData.setIntField10(chronicleQueueData.getIntField10());
                iChronicleQueueData.setIntField11(chronicleQueueData.getIntField11());
                iChronicleQueueData.setIntField12(chronicleQueueData.getIntField12());
                iChronicleQueueData.setIntField13(chronicleQueueData.getIntField13());
                iChronicleQueueData.setIntField14(chronicleQueueData.getIntField14());
                iChronicleQueueData.setIntField15(chronicleQueueData.getIntField15());
                iChronicleQueueData.setIntField16(chronicleQueueData.getIntField16());
                iChronicleQueueData.setIntField17(chronicleQueueData.getIntField17());
                iChronicleQueueData.setIntField18(chronicleQueueData.getIntField18());
                iChronicleQueueData.setIntField19(chronicleQueueData.getIntField19());
                iChronicleQueueData.setIntField20(chronicleQueueData.getIntField20());
                appender.startExcerpt(iChronicleQueueData.maxSize());
                appender.write(iChronicleQueueData);
                appender.finish();
            }
            long endTime = System.nanoTime();
            writeToFile(i, startTime, endTime);
        }
        appender.close();
    }

    public void writeToFile(int run, long startTime, long endTime) throws Exception {
        System.out.println("WriteTime, " + run + ", " + (endTime - startTime) / noOfRecords + ", ns per record.");
    }
}
