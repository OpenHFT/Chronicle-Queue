package net.openhft.chronicle.queue.impl.ringbuffer;

import net.openhft.chronicle.queue.impl.DirectChronicleQueue;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.thread.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
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

    private static final Logger LOG = LoggerFactory.getLogger(ZippedDocumentAppender.class.getName());

    private final BytesRingBuffer q;
    private final DirectChronicleQueue chronicleQueue;

    public static final String ERR_MSG = "It not possible to zip more than " +
            "Integer.MAX_VALUE bytes in one go";

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
    public void append(@NotNull Bytes bytes) throws InterruptedException {

        //noinspection StatementWithEmptyBody
        while (!q.offer(bytes)) {
        }
    }

    /**
     * terminates the background thread
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
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
        private Bytes inputBuffer = ByteBufferBytes.wrap(ByteBuffer.wrap(input));

        @NotNull
        private Bytes outputBuffer = ByteBufferBytes.wrap(ByteBuffer.wrap(input));

        private Consumer() {
            this.input = new byte[]{};
            this.inputBuffer = ByteBufferBytes.wrap(ByteBuffer.wrap(input));
        }

        @Override
        public void run() {
            try {
                for (; ; ) {

                    if (Thread.currentThread().isInterrupted())
                        return;

                    Bytes value;

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
                LOG.error("", e);
            }

        }

        @NotNull
        @Override
        public Bytes provide(final long maxSize) {

            if (maxSize < inputBuffer.capacity())
                return inputBuffer.clear();

            if (maxSize > Integer.MAX_VALUE) {
                throw new IllegalStateException(ERR_MSG);
            }

            // resize the buffers
            this.input = new byte[(int) maxSize];
            this.inputBuffer = ByteBufferBytes.wrap(ByteBuffer.wrap(input));

            this.output = new byte[(int) maxSize];
            this.outputBuffer = ByteBufferBytes.wrap(ByteBuffer.wrap(output));

            return inputBuffer;
        }


    }

}
