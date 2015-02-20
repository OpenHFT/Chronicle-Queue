package net.openhft.chronicle.queue.stateless.bytes;

import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * todo : currently work in process
 *
 * Created by Rob Austin
 */
public interface StatelessRawBytesAppender {

    /**
     * @param bytes append bytes to the tail
     * @return returns the index of the newly created except
     */
    long appendExcept(@NotNull Bytes bytes);

    /**
     * @param bytes  append bytes to offset
     * @param offset where the data should be written
     */
    void append(long offset, @NotNull Bytes bytes);


    void append(@NotNull UnaryOperator<Bytes> unaryOperator);
}
