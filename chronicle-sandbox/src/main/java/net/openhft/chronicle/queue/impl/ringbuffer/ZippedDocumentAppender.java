/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://chronicle.software
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
import net.openhft.chronicle.queue.impl.DirectChronicleQueue;
import net.openhft.chronicle.threads.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.zip.Deflater;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * Create a background thread to zip the {@code bytes} before appending the zipped bytes to the
 * ChronicleQueue, in the meantime the bytes are held in a ring buffer.
 *
 * @author Rob Austin.
 */
public class ZippedDocumentAppender implements Closeable {

    public static final String ERR_MSG = "It not possible to zip more than " +
            "Integer.MAX_VALUE bytes in one go";
    @NotNull
    private final BytesRingBuffer q;
    @NotNull
    private final DirectChronicleQueue chronicleQueue;
    @NotNull
    private final ExecutorService qReader;

    /**
     * @param bytesRingBuffer a ring buffer to hold the bytes before they are zipped
     * @param chronicleQueue  the chronicle you wish to append the zipped bytes to
     */
    public ZippedDocumentAppender(@NotNull final BytesRingBuffer bytesRingBuffer,
                                  @NotNull final DirectChronicleQueue chronicleQueue) {
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
    public void append(@NotNull Bytes<?> bytes) throws InterruptedException {

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
     * used to consumer bytes out of the ring buffer, zip up the bytes using the {@code compresser} and write these
     * zipped bytes to {@code chronicleQueue}
     */
    private class Consumer implements BytesRingBuffer.BytesProvider, Runnable {

        @NotNull
        private Deflater compresser = new Deflater();

        @NotNull
        private byte[] input = new byte[]{};

        @NotNull
        private byte[] output = new byte[]{};

        @NotNull
        private Bytes<ByteBuffer> inputBuffer = Bytes.wrap(ByteBuffer.wrap(input));

        @NotNull
        private Bytes<ByteBuffer> outputBuffer = Bytes.wrap(ByteBuffer.wrap(input));

        private Consumer() {
            this.input = new byte[]{};
            this.inputBuffer = Bytes.wrap(ByteBuffer.wrap(input));
        }

        @Override
        public void run() {
            try {
                for (; ; ) {
                    if (Thread.currentThread().isInterrupted())
                        return;

                    Bytes<?> value;

                    do {
                        value = q.poll(this);
                    } while (value == null);

                    compresser.setInput(input, (int) value.position(), (int) value.remaining());
                    compresser.finish();

                    int limit = compresser.deflate(output);
                    compresser.end();

                    outputBuffer.position(0);
                    outputBuffer.limit(limit);

                    chronicleQueue.appendDocument(outputBuffer);
                }
            } catch (Exception e) {
                Jvm.error().on(getClass(), "", e);
            }
        }

        @NotNull
        @Override
        public Bytes<?> provide(final long maxSize) {
            if (maxSize < inputBuffer.capacity())
                return inputBuffer.clear();

            if (maxSize > Integer.MAX_VALUE) {
                throw new IllegalStateException(ERR_MSG);
            }

            // resize the buffers
            this.input = new byte[(int) maxSize];
            this.inputBuffer = Bytes.wrap(ByteBuffer.wrap(input));

            this.output = new byte[(int) maxSize];
            this.outputBuffer = Bytes.wrap(ByteBuffer.wrap(output));

            return inputBuffer;
        }
    }
}
