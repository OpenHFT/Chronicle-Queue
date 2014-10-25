/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package demo;

import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.VanillaChronicleConfig;
import net.openhft.chronicle.tcp.ChronicleSink;
import net.openhft.chronicle.tcp.ChronicleSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by daniel on 19/06/2014.
 */
public class ChronicleController {
    private VanillaChronicle chronicle;
    private ExcerptTailer tailer;
    private ExcerptTailer tcpTailer;
    private ChronicleUpdatable updatable;
    private WriterThread writerThread1, writerThread2;
    private ReaderThread readerThread;
    private TCPReaderThread tcpReaderThread;
    private String BASE_PATH;
    private String BASE_PATH_SINK;
    private TimerThread timerThread;

    public ChronicleController(ChronicleUpdatable updatable, File demo_path) throws IOException {
        BASE_PATH = demo_path + "/source";
        BASE_PATH_SINK = demo_path + "/sink";
        this.updatable = updatable;
        reset();

        writerThread1 = new WriterThread("EURUSD", updatable.count1());
        writerThread1.start();
        writerThread2 = new WriterThread("USDCHF", updatable.count2());
        writerThread2.start();
        readerThread = new ReaderThread();
        readerThread.start();
        tcpReaderThread = new TCPReaderThread();
        tcpReaderThread.start();
        timerThread = new TimerThread();
        timerThread.start();
    }

    public void reset() {

        try {
            VanillaChronicleConfig config = new VanillaChronicleConfig();
            config.indexBlockSize(32 << 20);
            config.dataBlockSize(128 << 20);
            chronicle = new VanillaChronicle(BASE_PATH, config);
            ChronicleSource source = new ChronicleSource(chronicle, 0);
            ChronicleSink sink = new ChronicleSink(new VanillaChronicle(BASE_PATH_SINK), "localhost", source.getLocalPort());
            chronicle.clear();
            sink.clear();

            tailer = chronicle.createTailer();


            tcpTailer = sink.createTailer();
        } catch (IOException e) {
            e.printStackTrace();
        }

        updateFileNames();
    }

    public void start(String srate) {
        if (chronicle == null) reset();
        int rate = srate.equals("MAX") ? Integer.MAX_VALUE : Integer.valueOf(srate.trim().replace(",", ""));
        writerThread1.setRate(rate / 2);
        writerThread1.go();
        writerThread2.setRate(rate / 2);
        writerThread2.go();
        readerThread.go();
        tcpReaderThread.go();
        timerThread.go();
    }

    public void stop() {
        writerThread1.pause();
        writerThread2.pause();
        timerThread.pause();
        readerThread.pause();
        tcpReaderThread.pause();
    }

    public void updateFileNames() {
        Path dir = FileSystems.getDefault().getPath(BASE_PATH);
        updatable.setFileNames(getFileNames(new ArrayList<String>(), dir));
    }

    private List<String> getFileNames(List<String> fileNames, Path dir) {
        try {
            DirectoryStream<Path> stream = null;
            try {
                stream = Files.newDirectoryStream(dir);
            } catch (NoSuchFileException nsfe) {
                return fileNames;
            }

            for (Path path : stream) {
                if (path.toFile().isDirectory()) getFileNames(fileNames, path);
                else {
                    fileNames.add(path.toAbsolutePath().toString());
                }
            }
            stream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileNames;
    }

    private class WriterThread extends Thread {
        private static final int BATCH = 16;
        public static final int ELASTICITY = 1000000;
        private final String symbol;
        private final AtomicBoolean isRunning = new AtomicBoolean(false);
        private final AtomicLong count;
        private final VanillaChronicle.VanillaAppender appender;
        private int rate;

        public WriterThread(String symbol, AtomicLong count) throws IOException {
            this.symbol = symbol;
            this.count = count;
            appender = chronicle.createAppender();
        }


        public void run() {
            Price price = new Price(symbol, 1.1234, 2000000, 1.1244, 3000000, true);
            long startTime = 0/*, reportTime = 0, lastCount = 0*/;
            while (!Thread.interrupted()) {
                if (!isRunning.get()) {
                    msleep(100);
                    continue;
                } else if (startTime == 0) {
                    startTime = System.currentTimeMillis();
//                    reportTime = startTime + 1000;
                }
                long countWritten = count.get();
                for (int i = 0; i < BATCH; i++) {
                    double v = ((countWritten + i) & 31) / 1e4;
                    price.askPrice = 1.1234 + v;
                    price.bidPrice = 1.1244 + v;
                    writeMessage(price);
                }
                countWritten = count.addAndGet(BATCH);

                // give the replication a break.
                long diff = countWritten * 2 - updatable.tcpMessageRead().get();
                if (diff > ELASTICITY) {
                    pause(diff - ELASTICITY);
                }

                if (rate == Integer.MAX_VALUE)
                    continue;

                long now = System.currentTimeMillis();
//                if (now >= reportTime) {
//                    long countDone = countWritten - lastCount;
////                    System.out.println(countDone);
//                    lastCount = countWritten;
//                    reportTime += 1000;
//                }
                long runtime = now - startTime;
                long targetCount = runtime * rate / 990;
                if (countWritten > targetCount) {
                    pause(countWritten - targetCount - 100);
                }
            }
        }

        private void pause(long overrun) {
            if (overrun > 0)
                msleep(1);
            else
                Thread.yield();
        }

        private void writeMessage(Price price) {
            appender.startExcerpt(128);
            price.writeMarshallable(appender);
            appender.finish();
        }

        public void pause() {
            isRunning.set(false);
        }

        public void go() {
            isRunning.set(true);
        }

        public void setRate(int rate) {
            this.rate = rate;
        }
    }

    private class ReaderThread extends Thread {
        private AtomicBoolean isRunning = new AtomicBoolean(false);

        public void run() {
            while (true) {
                if (isRunning.get()) {
                    if (tailer.nextIndex()) {
                        updatable.incrMessageRead();
                    }
                } else {
                    msleep(100);
                }
            }
        }

        public void pause() {
            isRunning.set(false);
        }

        public void go() {
            isRunning.set(true);
        }
    }

    private class TCPReaderThread extends Thread {
        private AtomicBoolean isRunning = new AtomicBoolean(false);

        public void run() {
            Price p = new Price();
            while (true) {
                if (isRunning.get()) {
                    if (tcpTailer.nextIndex()) {
                        p.readMarshallable(tcpTailer);
                        updatable.incrTcpMessageRead();
                    }
                } else {
                    msleep(100);
                }
            }
        }

        public void pause() {
            isRunning.set(false);
        }

        public void go() {
            isRunning.set(true);
        }
    }

    private class TimerThread extends Thread {
        private AtomicBoolean isRunning = new AtomicBoolean(false);
        private boolean restart = true;
        private int count = 0;

        public void run() {
            while (true) {
                if (isRunning.get()) {
                    long startTime = System.currentTimeMillis();
                    msleep(100);

                    if (restart || count % 50 == 0) {
                        count = 0;
                        updateFileNames();
                        restart = false;
                    }
                    count++;

                    updatable.addTimeMillis(System.currentTimeMillis() - startTime);
                } else {
                    msleep(100);
                }
            }
        }

        public void pause() {
            isRunning.set(false);
        }

        public void go() {
            restart = true;
            isRunning.set(true);
        }
    }


    public void msleep(int time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
