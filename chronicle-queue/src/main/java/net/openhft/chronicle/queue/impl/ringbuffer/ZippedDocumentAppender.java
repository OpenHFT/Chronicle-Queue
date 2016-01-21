/*
 * Copyright 2015 Higher Frequency Trading
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

package net.openhft.chronicle.queue.impl.ringbuffer;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.threads.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.zip.Deflater;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static net.openhft.chronicle.bytes.BytesStore.wrap;

/**
 * Create a background thread to zip the {@code bytes} before appending the zipped bytes to the
 * ChronicleQueue, in the meantime the bytes are held in a ring buffer.
 *
 * @author Rob Austin.
 */
public class ZippedDocumentAppender implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ZippedDocumentAppender.class.getName());

    @NotNull
    private final BytesRingBuffer q;
    @NotNull
    private final ChronicleQueue chronicleQueue;

    public static final String ERR_MSG = "It not possible to zip more than " +
            "Integer.MAX_VALUE bytes in one go";

    @NotNull
    private final ExecutorService qReader;

    /**
     * @param bytesRingBuffer a ring buffer to hold the bytes before they are zipped
     * @param chronicleQueue  the chronicle you wish to append the zipped bytes to
     */
    public ZippedDocumentAppender(@NotNull final BytesRingBuffer bytesRingBuffer,
                                  @NotNull final ChronicleQueue chronicleQueue) {
        this.q = bytesRingBuffer;
        this.chronicleQueue = chronicleQueue;
        qReader = newSingleThreadExecutor(new NamedThreadFactory("qReader"));
        qReader.submit(new Consumer());
    }

    /**
     * the bytes that you wish to append, this bytes will become zipped and appended to the
     * chronicle using a background thread
     *
     * @param bytes the bytes to append
     * @throws InterruptedException
     */
    public void append(@NotNull Bytes bytes) throws InterruptedException {

        //noinspection StatementWithEmptyBody
        while (!q.offer(bytes)) {
        }
    }

    /**
     * terminates the background thread
     */
    @Override
    public void close() {
        qReader.shutdown();
    }

    /**
     * used to consumer bytes out of the ring buffer, zip up the bytes using the {@code compresser}
     * and writeBytes these zipped bytes to {@code chronicleQueue}
     */
    private class Consumer implements Runnable {

        @NotNull
        private Deflater compresser = new Deflater();

        @NotNull
        private byte[] input = new byte[]{};

        @NotNull
        private byte[] output = new byte[]{};

        @NotNull
        private Bytes<ByteBuffer> inputBuffer;

        {
            inputBuffer = wrap(ByteBuffer.wrap(input)).bytesForRead();
        }

        @NotNull
        private Bytes<ByteBuffer> outputBuffer = wrap(ByteBuffer.wrap(input)).bytesForWrite();

        private Consumer() {
            this.input = new byte[]{};

            this.inputBuffer = wrap(ByteBuffer.wrap(input)).bytesForRead();
        }

        @Override
        public void run() {
            try {
                for (; ; ) {

                    if (Thread.currentThread().isInterrupted())
                        return;

                    Bytes value;

                   /*

                   TODO ADD BACK IN **************

                   do {
                        value = q.read(this);
                    } while (value == null);


                    compresser.setInput(input, (int) value.readPosition(), (int) value.readRemaining());
                    compresser.finish();

                    int limit = compresser.deflate(output);
                    compresser.end();

                    outputBuffer.writePosition(0);
                    outputBuffer.writeLimit(limit);
                    ExcerptAppender appender = chronicleQueue.createAppender();
                    appender.writeDocument(w -> {
                        w.write(() -> "zipped").bytes(outputBuffer);

                    });
                          */
                }
            } catch (Exception e) {
                LOG.error("", e);
            }
        }

        @NotNull
        public Bytes provide(final long maxSize) {

            if (maxSize < inputBuffer.capacity())
                return inputBuffer.clear();

            if (maxSize > Integer.MAX_VALUE) {
                throw new IllegalStateException(ERR_MSG);
            }

            // resize the buffers
            this.input = new byte[(int) maxSize];
            this.inputBuffer = wrap(ByteBuffer.wrap(input)).bytesForRead();

            this.output = new byte[(int) maxSize];
            this.outputBuffer = wrap(ByteBuffer.wrap(output)).bytesForWrite();

            return inputBuffer;
        }
    }
}

