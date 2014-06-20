package demo;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.tcp.VanillaChronicleSink;
import net.openhft.chronicle.tcp.VanillaChronicleSource;

import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by daniel on 19/06/2014.
 */
public class ChronicleController {
    private VanillaChronicle chronicle;
    private ExcerptAppender appender;
    private ExcerptTailer tailer;
    private ExcerptTailer tcpTailer;
    private ChronicleUpdatable updatable;
    private WriterThread writerThread;
    private ReaderThread readerThread;
    private TCPReaderThread tcpReaderThread;
    private String BASE_PATH;
    private String BASE_PATH_SINK;
    private TimerThread timerThread;

    public ChronicleController(ChronicleUpdatable updatable){
        BASE_PATH = System.getProperty("java.io.tmpdir") + "demo";
        BASE_PATH_SINK = System.getProperty("java.io.tmpdir") + "demo/sink";
        this.updatable = updatable;
        writerThread = new WriterThread();
        writerThread.start();
        readerThread = new ReaderThread();
        readerThread.start();
        tcpReaderThread = new TCPReaderThread();
        tcpReaderThread.start();
        timerThread = new TimerThread();
        timerThread.start();
    }

    public void reset() {

        try
        {
            chronicle = new VanillaChronicle(BASE_PATH);
            VanillaChronicleSource source = new VanillaChronicleSource(chronicle, 0);
            VanillaChronicleSink sink = new VanillaChronicleSink(new VanillaChronicle(BASE_PATH_SINK), "localhost", source.getLocalPort());
            chronicle.clear();
            sink.clear();

            appender = chronicle.createAppender();
            tailer = chronicle.createTailer();


            tcpTailer = sink.createTailer();
        }catch(IOException e){
            e.printStackTrace();
        }

        updateFileNames();
    }

    public void start(String srate) {
        if (chronicle == null)reset();
        int rate = srate.equals("MAX") ? Integer.MAX_VALUE : Integer.valueOf(srate);
        readerThread.go();
        tcpReaderThread.go();
        writerThread.setRate(rate);
        writerThread.go();
        timerThread.go();
    }

    public void stop(){
        writerThread.pause();
        timerThread.pause();
        readerThread.pause();
        tcpReaderThread.pause();
    }

    public void updateFileNames(){
        Path dir = FileSystems.getDefault().getPath(BASE_PATH);
        updatable.setFileNames(getFileNames(new ArrayList<String>(), dir));
    }

    private List<String> getFileNames(List<String> fileNames, Path dir){
        try {
            DirectoryStream<Path> stream = null;
            try {
                stream = Files.newDirectoryStream(dir);
            }catch (NoSuchFileException nsfe){
                return fileNames;
            }

            for (Path path : stream) {
                if(path.toFile().isDirectory())getFileNames(fileNames, path);
                else {
                    fileNames.add(path.toAbsolutePath().toString());
                }
            }
            stream.close();
        }catch(IOException e){
            e.printStackTrace();
        }
        return fileNames;
    }

    private class WriterThread extends Thread{
        private AtomicBoolean isRunning = new AtomicBoolean(false);
        private int rate;
        private long count = 0;

        public void run() {
            while(true) {
                if(isRunning.get()){
                    if(rate!=Integer.MAX_VALUE) {
                        long startTime = System.currentTimeMillis();
                        for (int i = 0; i < rate; i++) {
                            writeMessage();
                        }
                        long timeToFinishBatch = System.currentTimeMillis() - startTime;

                        if (timeToFinishBatch < 1000) {
                            msleep(1000 - (int) timeToFinishBatch);
                        }
                    }
                    else{
                        writeMessage();
                    }
                }else{
                    msleep(100);
                }
            }
        }

        private void writeMessage() {
            appender.startExcerpt();
            appender.writeObject(new Price("PID_100", count, true));
            //appender.writeLong(count);
            appender.finish();
            updatable.messageProduced();
            count++;
        }

        public void pause(){
            isRunning.set(false);
        }

        public void go(){
            isRunning.set(true);
        }

        public void setRate(int rate) {
            this.rate = rate;
        }
    }

    private class ReaderThread extends Thread{
        private AtomicBoolean isRunning = new AtomicBoolean(false);

        public void run() {
            while(true) {
                if(isRunning.get()){
                    if(tailer.nextIndex()) {
                       updatable.messageRead();
                    }
                }else{
                    msleep(100);
                }
            }
        }

        public void pause(){
            isRunning.set(false);
        }

        public void go(){
            isRunning.set(true);
        }
    }

    private class TCPReaderThread extends Thread{
        private AtomicBoolean isRunning = new AtomicBoolean(false);

        public void run() {
            while(true) {
                if(isRunning.get()){
                    if(tcpTailer.nextIndex()) {
                        long val = tcpTailer.readLong();
                        updatable.tcpMessageRead();
                    }
                }else{
                    msleep(100);
                }
            }
        }

        public void pause(){
            isRunning.set(false);
        }

        public void go(){
            isRunning.set(true);
        }
    }

    private class TimerThread extends Thread{
        private AtomicBoolean isRunning = new AtomicBoolean(false);
        private boolean restart = true;
        private int count = 0;

        public void run() {
            while(true) {
                if(isRunning.get()){
                    long startTime = System.currentTimeMillis();
                    msleep(100);

                    if(restart || count % 50==0){
                        count = 0;
                        updateFileNames();
                        restart = false;
                    }
                    count++;

                    updatable.addTimeMillis(System.currentTimeMillis()-startTime);
                }else{
                    msleep(100);
                }
            }
        }

        public void pause(){
            isRunning.set(false);
        }

        public void go(){
            restart = true;
            isRunning.set(true);
        }
    }


    public void msleep(int time){
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
