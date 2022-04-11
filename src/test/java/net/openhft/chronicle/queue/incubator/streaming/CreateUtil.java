package net.openhft.chronicle.queue.incubator.streaming;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueOut;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

final class CreateUtil {

    private CreateUtil() {
    }

    @SafeVarargs
    @NotNull
    static SingleChronicleQueue createThenValueOuts(@NotNull final String name,
                                                    @NotNull final Consumer<ValueOut>... mutators) {
        final Consumer<SingleChronicleQueue> queueMutator = q -> {
            ExcerptAppender excerptAppender = q.acquireAppender();
            for (Consumer<ValueOut> mutator : mutators) {
                try (DocumentContext dc = excerptAppender.writingDocument()) {
                    mutator.accept(dc.wire().getValueOut());
                }
            }
        };
        return createThen(name, queueMutator);
    }

    @SafeVarargs
    @NotNull
    static SingleChronicleQueue createThenWritingDocuments(@NotNull final String name,
                                                           @NotNull final Consumer<DocumentContext>... mutators) {
        final Consumer<SingleChronicleQueue> queueMutator = q -> {
            ExcerptAppender excerptAppender = q.acquireAppender();
            for (Consumer<DocumentContext> mutator : mutators) {
                try (DocumentContext dc = excerptAppender.writingDocument()) {
                    mutator.accept(dc);
                }
            }
        };
        return createThen(name, queueMutator);
    }


    @NotNull
    static SingleChronicleQueue createThenAppending(@NotNull final String name,
                                                    @NotNull final Consumer<ExcerptAppender> mutator) {
        final Consumer<SingleChronicleQueue> queueMutator = q -> {
            ExcerptAppender excerptAppender = q.acquireAppender();
            mutator.accept(excerptAppender);
        };
        return createThen(name, queueMutator);
    }

    @NotNull
    static SingleChronicleQueue createThen(@NotNull final String name,
                                           @NotNull final Consumer<SingleChronicleQueue> queueMutator) {
        final SingleChronicleQueue q = createQueue(name);
        queueMutator.accept(q);
        return q;
    }

    @NotNull
    static SingleChronicleQueue createQueue(@NotNull final String name) {
        final SetTimeProvider tp = new SetTimeProvider(TimeUnit.HOURS.toNanos(365 * 24)).autoIncrement(1, TimeUnit.SECONDS);
        return SingleChronicleQueueBuilder
                .single(name)
                .timeProvider(tp)
                .build();
    }


}