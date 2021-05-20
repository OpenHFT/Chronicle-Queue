package net.openhft.chronicle.queue.benchmark;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.bytes.MethodReaderInterceptorReturns;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.InvocationTargetRuntimeException;
import net.openhft.chronicle.wire.*;

import java.util.function.Supplier;

public class BookUpdateWithQueueJLBHMethodReader extends AbstractGeneratedMethodReader {
    // instances on which parsed calls are invoked
    private final Object instance0;

    // run
    private long runarg0;

    // init
    private net.openhft.chronicle.core.jlbh.JLBH initarg0;

    // bookUpdate
    private net.openhft.chronicle.queue.benchmark.BookUpdate bookUpdatearg0;

    public BookUpdateWithQueueJLBHMethodReader(MarshallableIn in, WireParselet debugLoggingParselet, Supplier<MethodReader> delegateSupplier, MethodReaderInterceptorReturns interceptor, Object... instances) {
        super(in, debugLoggingParselet, delegateSupplier);
        instance0 = instances[0];
    }

    @Override
    protected boolean readOneCall(WireIn wireIn) {
        String lastEventName = "";
        if (wireIn.bytes().peekUnsignedByte() == BinaryWireCode.FIELD_NUMBER) {
            int methodId = (int) wireIn.readEventNumber();

            if (methodId == 1) {
                ValueIn valueIn = wireIn.getValueIn();
                bookUpdatearg0 = valueIn.object(checkRecycle(bookUpdatearg0), net.openhft.chronicle.queue.benchmark.BookUpdate.class);
                try {
                    ((net.openhft.chronicle.queue.benchmark.BookUpdateListener) instance0).bookUpdate(bookUpdatearg0);
                    return true;
                } catch (Exception e) {
                    throw new InvocationTargetRuntimeException(e);
                }
            } else
                return false;

        } else {
            lastEventName = wireIn.readEvent(String.class);
        }
        ValueIn valueIn = wireIn.getValueIn();
        try {


            if (Jvm.isDebug())
                debugLoggingParselet.accept(lastEventName, valueIn);
            if (lastEventName == null)
                throw new IllegalStateException("Failed to read method name or ID");


            switch (lastEventName) {
                case MethodReader.HISTORY:
                    valueIn.marshallable(messageHistory);
                    break;

                case "warmedUp":
                    valueIn.skipValue();
                    try {
                        ((net.openhft.chronicle.core.jlbh.JLBHTask) instance0).warmedUp();
                    } catch (Exception e) {
                        throw new InvocationTargetRuntimeException(e);
                    }
                    break;

                case "complete":
                    valueIn.skipValue();
                    try {
                        ((net.openhft.chronicle.core.jlbh.JLBHTask) instance0).complete();
                    } catch (Exception e) {
                        throw new InvocationTargetRuntimeException(e);
                    }
                    break;

                case "run":
                    runarg0 = valueIn.int64();
                    try {
                        ((net.openhft.chronicle.core.jlbh.JLBHTask) instance0).run(runarg0);
                    } catch (Exception e) {
                        throw new InvocationTargetRuntimeException(e);
                    }
                    break;

                case "init":
                    initarg0 = valueIn.object(checkRecycle(initarg0), net.openhft.chronicle.core.jlbh.JLBH.class);
                    try {
                        ((net.openhft.chronicle.core.jlbh.JLBHTask) instance0).init(initarg0);
                    } catch (Exception e) {
                        throw new InvocationTargetRuntimeException(e);
                    }
                    break;

                case "bookUpdate":
                    bookUpdatearg0 = valueIn.object(checkRecycle(bookUpdatearg0), net.openhft.chronicle.queue.benchmark.BookUpdate.class);
                    try {
                        ((net.openhft.chronicle.queue.benchmark.BookUpdateListener) instance0).bookUpdate(bookUpdatearg0);
                    } catch (Exception e) {
                        throw new InvocationTargetRuntimeException(e);
                    }
                    break;

                default:
                    return false;
            }
            return true;
        } catch (InvocationTargetRuntimeException e) {
            throw e;
        } catch (Exception e) {
            Jvm.warn().on(this.getClass(), "Failure to dispatch message, will retry to process without generated code: " + lastEventName + "(), bytes: " + wireIn.bytes().toDebugString(), e);
            return false;
        }
    }
}