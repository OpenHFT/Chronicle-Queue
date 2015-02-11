package vanilla.java.proxy;

import net.openhft.chronicle.ExcerptTailer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class FromChronicle<T> {
    private final T instance;
    private final ExcerptTailer tailer;
    private final Map<String, Method> methodMap = new HashMap<>();

    public FromChronicle(T instance, ExcerptTailer tailer) {
        this.instance = instance;
        this.tailer = tailer;
        for (Method m : instance.getClass().getMethods())
            if (m.getDeclaringClass() != Object.class)
                methodMap.put(m.getName(), m);
    }

    public void readOne() {
        if (tailer.nextIndex()) {
            MetaData.get().readMarshallable(tailer);
            String metaName = tailer.readUTFΔ();
            Method m = findMethod(metaName);
            Object[] args = null;
            int len = (int) tailer.readStopBit();
            if (len > 0) {
                for (int i = 0; i < len; i++)
                    args[i] = tailer.readObject();
            }
            try {
                m.invoke(instance, args);
            } catch (IllegalAccessException e) {
                throw new AssertionError(e);
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }

    private Method findMethod(String metaName) {
        return methodMap.get(metaName);
    }
}
