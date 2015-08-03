/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 */
package net.openhft.chronicle.tcp;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.tcp.network.SessionDetailsProvider;
import net.openhft.chronicle.tcp.network.TcpHandler;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.ByteBuffer.allocateDirect;
import static net.openhft.chronicle.ChronicleQueueBuilder.remoteTailer;
import static net.openhft.chronicle.ChronicleQueueBuilder.vanilla;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Test to illustrate a very basic TcpHandler in a pipeline
 */
public class ChronicleTcpPipelineTest extends ChronicleTcpTestBase {

    private static final Logger log = LoggerFactory.getLogger(ChronicleTcpPipelineTest.class);

    @Test
    public void recordingTest() throws IOException {
        final PortSupplier portSupplier = new PortSupplier();
        final int termination = 10; // timeout for termination of the appender/tailer
        final int messages = 100;
        final int timeout = 5; // timeout for reading nextIndex
        final AtomicInteger messagesRead = new AtomicInteger(0);
        final int expectedReadMessages = messages;
        final RecordingTcpHandler sourceRecorder = new RecordingTcpHandler("source");
        final RecordingTcpHandler remoteRecorder = new RecordingTcpHandler("remote");

        try (
                Chronicle source = vanilla(getVanillaTestPath())
                        .source()
                        .bindAddress(0)
                        .connectionListener(portSupplier)
                        .tcpHandlers(sourceRecorder)
                        .build();

                final Chronicle remote = remoteTailer()
                        .connectAddress("localhost", portSupplier.port())
                        .tcpHandlers(remoteRecorder)
                        .build();) {

            final ExecutorService executorService = Executors.newFixedThreadPool(2);
            executeAppender(messages, source, executorService);
            executeTailer(messages, timeout, messagesRead, remote, executorService);


            executorService.shutdown();
            try {
                executorService.awaitTermination(termination, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // do nothing
            }

            // check the messages have been read
            assertThat(messagesRead.get(), equalTo(expectedReadMessages));

            sourceRecorder.flip();
            remoteRecorder.flip();

            // check that what was sent was recorded as received.
            assertThat(remoteRecorder.inCopy.limit(), equalTo(sourceRecorder.outCopy.limit()));
            assertThat(sourceRecorder.inCopy.limit(), equalTo(remoteRecorder.outCopy.limit()));
        }
    }

    private void executeTailer(final int messages, final int timeout, final AtomicInteger messagesRead, final Chronicle remote, ExecutorService executorService) {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    final ExcerptTailer tailer = remote.createTailer();
                    for (int i = 0; i < messages; i++) {
                        final long start = System.currentTimeMillis();
                        long now = 0;
                        while (!tailer.nextIndex() && TimeUnit.MILLISECONDS.toSeconds((now = System.currentTimeMillis()) - start) < timeout); // spin

                        if (TimeUnit.MILLISECONDS.toSeconds(now - start) >= timeout) {
                            fail("Timed out getting next index.");
                        }

                        int idx = tailer.readInt();
                        char separator = tailer.readChar();
                        int val = tailer.readInt();

                        assertThat(idx, equalTo(i));

                        messagesRead.incrementAndGet();

                    }
                } catch (IOException e) {
                    log.error("Unexpected ioe.", e);
                    fail("Unexpected ioe.");
                }

            }
        });
    }

    private void executeAppender(final int messages, final Chronicle source, ExecutorService executorService) {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    final ExcerptAppender appender = source.createAppender();
                    for (int i = 0; i < messages; i++) {
                        appender.startExcerpt(1024);
                        appender.writeInt(i);
                        appender.writeChar(':');
                        appender.writeInt(i);
                        appender.finish();
                    }
                } catch (IOException e) {
                    log.error("Unexpected ioe.", e);
                    fail("Unexpected ioe.");
                }
            }
        });
    }

    /**
     * A very basic TcpHandler that simply records everything that downstream handlers read and write
     *
     * It's admittedly pointless but meant to be an illustration.
     */
    private class RecordingTcpHandler implements TcpHandler {

        private final Bytes inCopy = ByteBufferBytes.wrap(allocateDirect((1 << 20)));

        private final Bytes outCopy = ByteBufferBytes.wrap(allocateDirect((1 << 20)));;

        private final BusyChecker busyChecker = new BusyChecker();

        private final String name;

        public RecordingTcpHandler(String name) {
            this.name = name;
        }

        @Override
        public boolean process(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
            busyChecker.mark(in, out);
            long inCopyPos = inCopy.position();
            if (in.remaining() > 0) {
                // copy everything into the in buffer
                long inPos = in.position();
                inCopy.write(in);
                in.position(inPos);
            }

            long preOutPos = out.position();
            long preInPos = in.position();
            PipelineContext pipelineContext = sessionDetailsProvider.get(PipelineContext.class);
            if (pipelineContext != null) {
                // call the next handler in the pipeline
                pipelineContext.next(in, out, sessionDetailsProvider);
            }

            long postInPos = in.position();
            if (postInPos > preInPos) {
                // wind back the in buffer to the position the downstream read up until
                long bytesRead = postInPos - preInPos;
                inCopy.position(inCopyPos + bytesRead);
            }

            long postOutPos = out.position();
            if (postOutPos > preOutPos) {
                // copy everything we wrote
                long bytesWritten = postOutPos - preOutPos;
                outCopy.write(out, preOutPos, bytesWritten);
            }
            return busyChecker.busy(in, out);
        }

        @Override
        public void onEndOfConnection(SessionDetailsProvider sessionDetailsProvider) {

        }

        public void flip() {
            outCopy.flip();
            inCopy.flip();
        }
    }

}
