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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by daniel on 19/06/2014.
 */
public class ChronicleController {
    private Chronicle chronicle;
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
    }

    public void start(String srate) throws IOException {
        chronicle = ChronicleQueueBuilder.vanilla(BASE_PATH)
            .indexBlockSize(32 << 20)
            .dataBlockSize(128 << 20)
            .build();

        int rate = srate.equals("MAX") ? Integer.MAX_VALUE : Integer.valueOf(srate.trim().replace(",", ""));
        writerThread1 = new WriterThread("EURUSD", updatable.count1(), rate/2);
        writerThread1.start();
        writerThread2 = new WriterThread("USDCHF", updatable.count2(), rate/2);
        writerThread2.start();
        readerThread = new ReaderThread();
        readerThread.start();
        tcpReaderThread = new TCPReaderThread();
        tcpReaderThread.start();
        timerThread = new TimerThread();
        timerThread.start();
        writerThread1.go();
        writerThread2.go();
        readerThread.go();
        tcpReaderThread.go();
        timerThread.go();
    }

    public void stop() {
        writerThread1.exit();
        writerThread2.exit();
        readerThread.exit();
        tcpReaderThread.exit();
        timerThread.exit();
        chronicle.clear();

        try {
            chronicle.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    private List<String> getFileNames(List<String> fileNames, Path dir) {
        try {
            DirectoryStream<Path> stream;
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

    private class WriterThread extends ControlledThread{
        private static final int BATCH = 16;
        public static final int ELASTICITY = 1000000;
        private final AtomicLong count;
        private final ExcerptAppender appender;
        private int rate;
        private Price price = null;
        private long startTime = 0;

        public WriterThread(String symbol, AtomicLong count, int rate) throws IOException {
            this.rate = rate;
            this.count = count;
            appender = chronicle.createAppender();
            price = new Price(symbol, 1.1234, 2000000, 1.1244, 3000000, true);
        }
        @Override
        public void loop(){
            if(startTime== 0)startTime = System.currentTimeMillis();

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

            if (rate == Integer.MAX_VALUE) return;

            long now = System.currentTimeMillis();

            long runtime = now - startTime;
            long targetCount = runtime * rate / 990;
            if (countWritten > targetCount) {
                pause(countWritten - targetCount - 100);
            }
        }

        private void pause(long overrun) {
            if (overrun > 0)
                sleepnx(5);
            else
                Thread.yield();
        }

        private void writeMessage(Price price) {
            appender.startExcerpt(128);
            price.writeMarshallable(appender);
            appender.finish();
        }

        @Override
        public void cleanup() {
            appender.close();
        }
    }

    private class ReaderThread extends ControlledThread {
        private ExcerptTailer tailer = null;

        public ReaderThread() throws IOException{
            tailer = chronicle.createTailer();
        }

        @Override
        public void loop() {
            if (tailer.nextIndex()) {
                updatable.incrMessageRead();
            }
        }

        @Override
        public void cleanup() {
            tailer.close();
        }
    }

    private class TCPReaderThread extends ControlledThread {
        private Price p = new Price();
        private ExcerptTailer tcpTailer = null;
        private Chronicle sink = null;
        private Chronicle source = null;

        public TCPReaderThread() throws IOException{
            source = ChronicleQueueBuilder.source(chronicle).bindAddress("localhost", 10987).build();
            sink = ChronicleQueueBuilder.vanilla(BASE_PATH_SINK).sink().connectAddress("localhost", 10987).build();
            tcpTailer = sink.createTailer();
        }

        @Override
        public void loop() {
            if (tcpTailer.nextIndex()) {
                p.readMarshallable(tcpTailer);
                updatable.incrTcpMessageRead();
            }
        }

        @Override
        public void cleanup() {
            tcpTailer.close();
            sink.clear();
            source.clear();

            try {
                sink.close();
                source.close();
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class TimerThread extends ControlledThread {
        private int count = 0;
        Path dir = FileSystems.getDefault().getPath(BASE_PATH);

        @Override
        public void loop() {
            long startTime = System.currentTimeMillis();
            sleepnx(100);

            if (count % 50 == 0) {
                count = 0;
                updatable.setFileNames(getFileNames(new ArrayList<String>(), dir));
            }
            count++;
            updatable.addTimeMillis(System.currentTimeMillis() - startTime);
        }

        @Override
        public void cleanup() {
        }
    }
}
