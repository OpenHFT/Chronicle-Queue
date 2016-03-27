package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueOut;
import net.openhft.chronicle.wire.Wire;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by peter on 25/03/16.
 */
class MethodWriterInvocationHandler implements InvocationHandler {
    private final ExcerptAppender appender;
    private final Map<Method, Class[]> parameterMap = new ConcurrentHashMap<>();

    MethodWriterInvocationHandler(ExcerptAppender appender) {
        this.appender = appender;
    }

    // Note the Object[] passed in creates an object on every call.
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getDeclaringClass() == Object.class) {
            return method.invoke(this, args);
        }
        try (DocumentContext context = appender.writingDocument()) {
            Wire wire = context.wire();

            ValueOut valueOut = wire
                    .writeEventName(method.getName());
            Class[] parameterTypes = parameterMap.get(method);
            if (parameterTypes == null)
                parameterMap.put(method, parameterTypes = method.getParameterTypes());
            switch (parameterTypes.length) {
                case 0:
                    valueOut.text("");
                    break;
                case 1:
                    valueOut.object(parameterTypes[0], args[0]);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        return ObjectUtils.defaultValue(method.getReturnType());
    }
}
