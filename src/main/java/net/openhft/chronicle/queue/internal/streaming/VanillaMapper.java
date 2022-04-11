package net.openhft.chronicle.queue.internal.streaming;

import net.openhft.chronicle.queue.incubator.streaming.Accumulation;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

final class VanillaMapper<T, R> implements Accumulation.MapperTo<R> {

    private final T delegate;
    private final Function<? super T, ? extends R> mapper;

    public VanillaMapper(@NotNull final T delegate,
                         @NotNull final Function<? super T, ? extends R> mapper) {
        this.delegate = delegate;
        this.mapper = mapper;
    }

    @Override
    public R map() {
        return mapper.apply(delegate);
    }

}