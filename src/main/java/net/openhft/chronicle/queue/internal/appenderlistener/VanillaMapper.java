package net.openhft.chronicle.queue.internal.appenderlistener;

import net.openhft.chronicle.queue.AppenderListener;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

final class VanillaMapper<T, R> implements AppenderListener.Accumulation.MapperTo<R> {

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